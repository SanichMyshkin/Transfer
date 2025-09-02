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


def fetch_remote_cert(nexus_url: str, remote_url: str, auth: tuple):
    """Получить сертификат для указанного URL"""
    parsed = urlparse(remote_url)
    host = parsed.hostname
    if not host or parsed.scheme != "https":
        logging.info(f"⏭️ {remote_url} не использует HTTPS")
        return None
    
    port = parsed.port or 443
    endpoint = f"security/ssl?host={host}&port={port}"
    certs = get_from_nexus(nexus_url, endpoint, auth)
    
    if isinstance(certs, dict):
        return certs
    elif isinstance(certs, list) and len(certs) > 0:
        return certs[0]  # возвращаем первый сертификат
    return None


def add_cert_to_truststore(
    nexus_url: str, auth: tuple, pem: str, cn: str, remote: str
):
    """Добавить сертификат в truststore"""
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
                f"🔄 JSON-вариант не сработал для CN='{cn}', пробуем raw PEM"
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
            logging.info(f"🔒 Сертификат CN='{cn}' уже есть для {remote}")
            return False
        resp.raise_for_status()
        logging.info(f"✅ Добавлен CN='{cn}' для {remote}")
        return True
    except Exception as e:
        logging.error(f"❌ Ошибка при добавлении CN='{cn}' для {remote}: {e}")
        return False


def add_single_cert_from_url(nexus_url: str, auth: tuple, target_url: str):
    """Добавить один сертификат по указанному URL"""
    logging.info(f"🔍 Получаем сертификат для URL: {target_url}")
    
    remote_cert = fetch_remote_cert(nexus_url, target_url, auth)
    if not remote_cert:
        logging.error(f"❌ Не удалось получить сертификат для {target_url}")
        return False
    
    remote_cn = remote_cert.get("subjectCommonName")
    pem = remote_cert.get("pem")
    if not remote_cn or not pem:
        logging.error(f"❌ Сертификат некорректный для {target_url}")
        return False
    
    logging.info(f"➕ Пробуем добавить CN='{remote_cn}' для {target_url}")
    return add_cert_to_truststore(nexus_url, auth, pem, remote_cn, target_url)


if __name__ == "__main__":
    load_dotenv()
    NEXUS_URL = os.getenv("NEXUS_URL")
    NEXUS_USER = os.getenv("NEXUS_USER")
    NEXUS_PASS = os.getenv("NEXUS_PASS")
    TARGET_URL = os.getenv("TARGET_URL")
    
    if not TARGET_URL:
        logging.error("❌ Переменная TARGET_URL не установлена!")
        logging.info("💡 Использование: установите TARGET_URL=https://example.com:port")
        exit(1)
    
    if not all([NEXUS_URL, NEXUS_USER, NEXUS_PASS]):
        logging.error("❌ Не установлены обязательные переменные: NEXUS_URL, NEXUS_USER, NEXUS_PASS")
        exit(1)
    
    auth = (NEXUS_USER, NEXUS_PASS)
    
    logging.info(f"🚀 Добавление сертификата для URL: {TARGET_URL}")
    success = add_single_cert_from_url(NEXUS_URL, auth, TARGET_URL)
    
    if success:
        logging.info("✅ Сертификат успешно добавлен")
    else:
        logging.error("❌ Не удалось добавить сертификат")
        exit(1)