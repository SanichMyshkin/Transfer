import requests
import urllib3
from urllib.parse import urlparse
from requests.exceptions import RequestException
from dotenv import load_dotenv
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36"
    )
}

# --- SSL/Cert config ---
NEXUS_CERT = os.getenv("NEXUS_CERT")  # путь до кастомного CA/cert
VERIFY = NEXUS_CERT if NEXUS_CERT else False

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=0)
session.mount("https://", adapter)
session.mount("http://", adapter)


def safe_get_json(url: str, auth: tuple, timeout: int = 20):
    try:
        response = session.get(
            url, auth=auth, headers=HEADERS, timeout=timeout, verify=VERIFY
        )
        if response.status_code == 400:
            logging.warning(f"❗ Nexus вернул 400 Bad Request при запросе {url}")
            return []
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logging.error(f"❌ Ошибка запроса {url}: {e}")
        return []


def get_from_nexus(nexus_url: str, endpoint: str, auth: tuple, timeout: int = 20):
    full_url = f"{nexus_url.rstrip('/')}/service/rest/v1/{endpoint.lstrip('/')}"
    return safe_get_json(full_url, auth, timeout)


def fetch_remote_certs(nexus_url: str, remote_url: str, auth: tuple, repo: str):
    parsed = urlparse(remote_url)
    host = parsed.hostname
    if not host or parsed.scheme != "https":
        logging.info(f"⏭️ Repo='{repo}': {remote_url} не использует HTTPS")
        return []
    port = parsed.port or 443
    endpoint = f"security/ssl?host={host}&port={port}"
    certs = get_from_nexus(nexus_url, endpoint, auth)
    if isinstance(certs, dict):
        return [certs]
    elif isinstance(certs, list):
        return certs
    return []


def add_cert_to_truststore(
    nexus_url: str, auth: tuple, pem: str, cn: str, remote: str, repo: str
):
    try:
        resp = requests.post(
            f"{nexus_url}/service/rest/v1/security/ssl/truststore",
            auth=auth,
            headers={"Content-Type": "application/json"},
            json={"certificate": pem},
            timeout=10,
            verify=VERIFY,
        )
        if resp.status_code == 400:
            logging.warning(
                f"🔄 Repo='{repo}': JSON-вариант не сработал для CN='{cn}', пробуем raw PEM"
            )
            resp = requests.post(
                f"{nexus_url}/service/rest/v1/security/ssl/truststore",
                auth=auth,
                headers={"Content-Type": "application/x-pem-file"},
                data=pem.encode(),
                timeout=10,
                verify=VERIFY,
            )
        if resp.status_code == 409:
            logging.info(
                f"🔒 Repo='{repo}': сертификат CN='{cn}' уже есть для {remote}"
            )
            return False
        resp.raise_for_status()
        logging.info(f"✅ Repo='{repo}': добавлен CN='{cn}' для {remote}")
        return True
    except Exception as e:
        logging.error(
            f"❌ Repo='{repo}': ошибка при добавлении CN='{cn}' для {remote}: {e}"
        )
        return False


def sync_remote_certs(nexus_url: str, auth: tuple):
    repos = get_from_nexus(nexus_url, "repositories", auth) or []
    logging.info(f"📦 Получено {len(repos)} репозиториев")
    repos = [
        {
            "name": r["name"],
            "remote": r.get("attributes", {}).get("proxy", {}).get("remoteUrl", ""),
        }
        for r in repos
        if r.get("type") == "proxy"
        and r.get("attributes", {})
        .get("proxy", {})
        .get("remoteUrl", "")
        .startswith("https://")
    ]
    for repo in repos:
        remote = repo["remote"]
        name = repo["name"]
        logging.info(f"🔍 Repo='{name}': проверяем {remote}")
        remote_certs = fetch_remote_certs(nexus_url, remote, auth, name)
        if not remote_certs:
            logging.warning(
                f"⚠️ Repo='{name}': не удалось получить сертификаты для {remote}"
            )
            continue
        for rc in remote_certs:
            remote_cn = rc.get("subjectCommonName")
            pem = rc.get("pem")
            if not remote_cn or not pem:
                logging.warning(
                    f"⚠️ Repo='{name}': сертификат некорректный для {remote}"
                )
                continue
            logging.info(
                f"➕ Repo='{name}': пробуем добавить CN='{remote_cn}' для {remote}"
            )
            add_cert_to_truststore(nexus_url, auth, pem, remote_cn, remote, name)


if __name__ == "__main__":
    load_dotenv()
    NEXUS_URL = os.getenv("NEXUS_URL")
    NEXUS_USER = os.getenv("NEXUS_USER")
    NEXUS_PASS = os.getenv("NEXUS_PASS")
    logging.info("🚀 Запуск синхронизации SSL сертификатов…")
    sync_remote_certs(NEXUS_URL, (NEXUS_USER, NEXUS_PASS))