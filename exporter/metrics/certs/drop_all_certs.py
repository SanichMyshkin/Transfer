import os
import requests
import urllib3
from requests.exceptions import SSLError, RequestException, ConnectionError
import logging

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36"
    )
}

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=0)
session.mount("https://", adapter)
session.mount("http://", adapter)


def safe_get_json(url: str, auth: tuple, timeout: int = 20):
    try:
        response = session.get(
            url, auth=auth, headers=HEADERS, timeout=timeout, verify=True
        )
        response.raise_for_status()
        return response.json()
    except SSLError as ssl_err:
        logging.warning(f"⚠️ SSL ошибка при запросе к {url}: {ssl_err}")
        try:
            response = session.get(
                url, auth=auth, headers=HEADERS, timeout=timeout, verify=False
            )
            logging.warning(f"⚠️ Использован verify=False для {url}")
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            logging.error(f"❌ Ошибка запроса без verify: {e}")
            return []
    except (ConnectionError, RequestException) as e:
        logging.error(f"❌ Ошибка подключения к {url}: {e}")
        return []


def get_from_nexus(nexus_url: str, endpoint: str, auth: tuple, timeout: int = 20):
    full_url = f"{nexus_url.rstrip('/')}/service/rest/v1/{endpoint.lstrip('/')}"
    return safe_get_json(full_url, auth, timeout)


def drop_all_certs(nexus_url: str, auth: tuple):
    """Удаляет все сертификаты из truststore Nexus."""
    try:
        truststore = get_from_nexus(nexus_url, "security/ssl/truststore", auth) or []
    except Exception as e:
        logging.error(f"❌ Ошибка при получении truststore: {e}")
        return

    if not truststore:
        logging.info("✅ Truststore пуст — удалять нечего")
        return

    logging.info(f"🔍 Найдено {len(truststore)} сертификатов в truststore")

    for cert in truststore:
        cert_id = cert.get("id")
        cn = cert.get("subjectCommonName", "???")

        if not cert_id:
            logging.warning(f"⚠️ Пропускаем сертификат без id (CN='{cn}')")
            continue

        try:
            resp = requests.delete(
                f"{nexus_url.rstrip('/')}/service/rest/v1/security/ssl/truststore/{cert_id}",
                auth=auth,
                timeout=10,
                verify=False,  # чтобы не упало на SSL
            )
            resp.raise_for_status()
            logging.info(f"🗑️ Удалён сертификат CN='{cn}', ID='{cert_id}'")
        except Exception as e:
            logging.error(f"❌ Ошибка при удалении CN='{cn}', ID='{cert_id}': {e}")

    logging.info("✔️ Все доступные сертификаты обработаны")


if __name__ == "__main__":
    NEXUS_URL = os.getenv("NEXUS_URL", "https://nexus.sanich.tech:8443")
    NEXUS_USER = os.getenv("NEXUS_USER", "admin")
    NEXUS_PASS = os.getenv("NEXUS_PASS", "admin123")

    drop_all_certs(NEXUS_URL, (NEXUS_USER, NEXUS_PASS))
