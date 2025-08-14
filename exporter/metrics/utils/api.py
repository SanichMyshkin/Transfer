from common.logs import logging
import requests
import urllib3
import urllib.parse
from common.config import NEXUS_API_URL
from requests.exceptions import SSLError, RequestException, ConnectionError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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


def get_from_nexus(nexus_url: str, endpoint: str, auth: tuple, timeout: int = 20):
    full_url = f"{nexus_url.rstrip('/')}/service/rest/v1/{endpoint.lstrip('/')}"
    return safe_get_json(full_url, auth, timeout)


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


def build_nexus_url(repo, image, encoding=True):
    path = f"v2/{image}/tags"
    if encoding:
        path = urllib.parse.quote(path, safe="")
    return f"{NEXUS_API_URL}#browse/browse:{repo}:{path}"


def safe_get_raw(url: str, auth: tuple = None, timeout: int = 20):
    try:
        response = session.get(
            url,
            auth=auth,
            headers=HEADERS,
            timeout=timeout,
            verify=True,
            allow_redirects=True,
        )
        return response, None
    except SSLError as ssl_err:
        logging.warning(f"⚠️ SSL ошибка при обращении к {url}: {ssl_err}")
        try:
            response = session.get(
                url,
                auth=auth,
                headers=HEADERS,
                timeout=timeout,
                verify=False,
                allow_redirects=True,
            )
            logging.warning(f"⚠️ Использован verify=False для {url}")
            return response, None
        except RequestException as e:
            logging.warning(f"❌ Ошибка (без verify) при обращении к {url}: {e}")
            return None, e
    except ConnectionError as e:
        logging.warning(f"❌ Ошибка подключения к {url}: {e}")
        return None, e
    except RequestException as e:
        logging.warning(f"❌ Ошибка запроса к {url}: {e}")
        return None, e


# # Узкоспециализированные обёртки (для читаемости)
# def get_certificates(nexus_url: str, auth: tuple) -> list:
#     return get_from_nexus(nexus_url, "security/ssl/truststore", auth)


# def get_repositories(nexus_url: str, auth: tuple) -> list:
#     return get_from_nexus(nexus_url, "repositories", auth)


# def get_tasks(nexus_url: str, auth: tuple) -> dict | list | None:
#     return get_from_nexus(nexus_url, "tasks", auth)
