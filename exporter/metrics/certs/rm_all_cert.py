import requests
from common.logs import logging
from metrics.utils.api import get_from_nexus


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
                f"{nexus_url}service/rest/v1/security/ssl/truststore/{cert_id}",
                auth=auth,
                timeout=10,
            )
            resp.raise_for_status()
            logging.info(f"🗑️ Удалён сертификат CN='{cn}', ID='{cert_id}'")
        except Exception as e:
            logging.error(f"❌ Ошибка при удалении CN='{cn}', ID='{cert_id}': {e}")

    logging.info("✔️ Все доступные сертификаты обработаны")
