import requests
from urllib.parse import urlparse
from common.logs import logging
from metrics.utils.api import get_from_nexus


def match_level(cert_cn: str, remote_url: str) -> int:
    if not cert_cn or not remote_url:
        return 0
    base = cert_cn.strip("*.")
    if base in remote_url:
        return 1
    short = base.split(".")[0]
    if short in remote_url:
        return 2
    return 0


def fetch_remote_certs(nexus_url: str, remote_url: str, auth: tuple, repo: str):
    """Получаем сертификаты с помощью Nexus API."""
    parsed = urlparse(remote_url)
    host = parsed.hostname
    if not host:
        logging.warning(f"⚠️ Repo='{repo}': не удалось извлечь host из {remote_url}")
        return []

    port = parsed.port or (443 if parsed.scheme == "https" else 80)

    endpoint = f"security/ssl?host={host}&port={port}"
    try:
        certs = get_from_nexus(nexus_url, endpoint, auth)
    except Exception as e:
        logging.error(
            f"❌ Repo='{repo}': ошибка при запросе host/port для {remote_url}: {e}, fallback…"
        )
        base_url = f"{parsed.scheme}://{host}"
        endpoint = f"security/ssl?url={base_url}"
        try:
            certs = get_from_nexus(nexus_url, endpoint, auth)
        except Exception as e2:
            logging.error(
                f"❌ Repo='{repo}': ошибка при запросе url для {remote_url}: {e2}"
            )
            return []

    if isinstance(certs, dict):
        return [certs]
    elif isinstance(certs, list):
        return certs
    return []


def add_cert_to_truststore(
    nexus_url: str, auth: tuple, pem: str, cn: str, remote: str, repo: str
):
    """Добавляем сертификат в truststore Nexus, пробуя оба варианта API."""
    try:
        # Пробуем JSON с ключом certificate
        resp = requests.post(
            f"{nexus_url}/service/rest/v1/security/ssl/truststore",
            auth=auth,
            headers={"Content-Type": "application/json"},
            json={"certificate": pem},
            timeout=10,
        )
        if resp.status_code == 400:
            logging.warning(
                f"⚠️ Repo='{repo}': JSON-вариант не сработал для CN='{cn}', пробуем raw PEM"
            )
            resp = requests.post(
                f"{nexus_url}/service/rest/v1/security/ssl/truststore",
                auth=auth,
                headers={"Content-Type": "application/x-pem-file"},
                data=pem.encode(),
                timeout=10,
            )
        resp.raise_for_status()
        logging.info(
            f"✅ Repo='{repo}': сертификат CN='{cn}' успешно добавлен для {remote}"
        )
        return True
    except Exception as e:
        logging.error(
            f"❌ Repo='{repo}': ошибка при добавлении сертификата CN='{cn}' для {remote}: {e}"
        )
        return False


def remove_duplicate_certs(nexus_url: str, auth: tuple, cleanup_duplicates: bool = True):
    """Удаляет дубликаты сертификатов из truststore (по fingerprint), если cleanup_duplicates=True."""
    try:
        truststore = get_from_nexus(nexus_url, "security/ssl/truststore", auth) or []
    except Exception as e:
        logging.error(f"❌ Ошибка при получении truststore: {e}")
        return

    if not cleanup_duplicates:
        logging.info("🟡 Удаление дубликатов отключено (cleanup_duplicates=False)")
        return

    seen = set()
    for cert in truststore:
        fp = cert.get("fingerprint")
        cid = cert.get("id")
        if not fp or not cid:
            continue

        if fp in seen:
            # дубликат → удаляем
            try:
                resp = requests.delete(
                    f"{nexus_url}/service/rest/v1/security/ssl/truststore/{cid}",
                    auth=auth,
                    timeout=10,
                )
                resp.raise_for_status()
                logging.info(
                    f"🗑️ Удалён дубликат сертификата CN='{cert.get('subjectCommonName')}' (fingerprint={fp})"
                )
            except Exception as e:
                logging.error(f"❌ Ошибка при удалении дубликата fingerprint={fp}: {e}")
        else:
            seen.add(fp)


def sync_remote_certs(nexus_url: str, auth: tuple, cleanup_duplicates: bool = True):
    """Подтягиваем и добавляем сертификаты для всех proxy-репозиториев."""
    try:
        truststore = get_from_nexus(nexus_url, "security/ssl/truststore", auth) or []
        repos = get_from_nexus(nexus_url, "repositories", auth) or []
    except Exception as e:
        logging.error(f"Ошибка при получении данных из Nexus: {e}")
        return

    logging.info(f"Получено {len(truststore)} сертификатов в truststore")
    logging.info(f"Получено {len(repos)} репозиториев")

    repos = [
        {
            "name": r["name"],
            "remote": r.get("attributes", {}).get("proxy", {}).get("remoteUrl", ""),
        }
        for r in repos
        if r.get("type") == "proxy"
        and r.get("attributes", {}).get("proxy", {}).get("remoteUrl")
    ]

    for repo in repos:
        remote = repo["remote"]
        name = repo["name"]

        # проверяем уже существующие сертификаты
        best_level = 0
        best_cert_cn = None
        for cert in truststore:
            cn = cert.get("subjectCommonName", "")
            level = match_level(cn, remote)
            if level > best_level:
                best_level = level
                best_cert_cn = cn

        # всегда подтягиваем свежие сертификаты у remote
        remote_certs = fetch_remote_certs(nexus_url, remote, auth, name)
        if not remote_certs:
            logging.warning(
                f"❌ Repo='{name}': не удалось получить сертификаты для {remote}"
            )
            continue

        for rc in remote_certs:
            remote_cn = rc.get("subjectCommonName")
            pem = rc.get("pem")
            if not remote_cn or not pem:
                continue

            # если в truststore такого CN ещё нет — добавляем
            if all(c.get("subjectCommonName") != remote_cn for c in truststore):
                logging.info(
                    f"➕ Repo='{name}': добавляем новый сертификат CN='{remote_cn}' для {remote}"
                )
                if add_cert_to_truststore(
                    nexus_url, auth, pem, remote_cn, remote, name
                ):
                    truststore.append(rc)
                    # после добавления чистим дубликаты (если включено)
                    remove_duplicate_certs(nexus_url, auth, cleanup_duplicates)
            else:
                logging.info(
                    f"✅ Repo='{name}': сертификат CN='{remote_cn}' уже есть для {remote}"
                )

            # обновляем уровень совпадения
            level = match_level(remote_cn, remote)
            if level > best_level:
                best_level = level
                best_cert_cn = remote_cn

        if best_level > 0 and best_cert_cn:
            logging.info(
                f"✔️ Repo='{name}': URL='{remote}', CN='{best_cert_cn}', Уровень={best_level}"
            )
        else:
            logging.info(f"⚠️ Repo='{name}': нет совпадений для URL='{remote}'")
