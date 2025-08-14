from common.logs import logging
import requests
import urllib3
from prometheus_client import Gauge

# Отключаем ворнинги и лишние логи от urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Prometheus метрика
BLOB_STORAGE_USAGE = Gauge(
    "nexus_blob_storage_usage",
    "Total used and available space in Nexus blob stores",
    ["blob_name", "metric_type", "blob_type", "blob_count", "blob_quota"],
)

BLOB_QUOTA = Gauge(
    "nexus_blob_quota",
    "The quota allocated for each blob",
    ["blob_name"],
)


def get_blobstores(nexus_url: str, auth: tuple) -> list | None:
    """Получает список blobstores из Nexus API."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        response = session.get(
            f"{nexus_url}/service/rest/v1/blobstores",
            auth=auth,
            verify=False,
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        logging.error(f"❌ Не удалось подключиться к Nexus: {nexus_url}")
    except requests.exceptions.Timeout:
        logging.error(f"⏳ Таймаут при подключении к Nexus: {nexus_url}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"⚠️ HTTP {e.response.status_code}: {e.response.reason}")
    except requests.exceptions.RequestException as e:
        logging.error(f"❗ Ошибка при запросе к Nexus: {e}")
    return None


def get_quota(data: dict):
    """Извлекает квоту если она есть"""
    quota = data.get("softQuota")
    return quota.get("limit") if quota else None


def update_metrics(blobstores: list) -> None:
    """Обновляет метрики Prometheus по полученным blobstores."""
    BLOB_STORAGE_USAGE.clear()
    BLOB_QUOTA.clear()
    for blob in blobstores:
        quota = get_quota(blob)

        BLOB_STORAGE_USAGE.labels(
            blob_name=blob["name"],
            metric_type="used",
            blob_count=str(blob["blobCount"]),
            blob_type=blob["type"],
            blob_quota=str(quota),
        ).set(blob["totalSizeInBytes"])

        BLOB_STORAGE_USAGE.labels(
            blob_name=blob["name"],
            metric_type="available",
            blob_count=str(blob["blobCount"]),
            blob_type=blob["type"],
            blob_quota=str(quota),
        ).set(blob["availableSpaceInBytes"])

        if quota:
            BLOB_QUOTA.labels(blob_name=blob.get("name")).set(int(quota))

        logging.info(
            f"[{blob['name']}] used: {blob['totalSizeInBytes']} | "
            f"available: {blob['availableSpaceInBytes']} | "
            f"type: {blob['type']} | count: {blob['blobCount']} | quota: {quota}"
        )


def fetch_blob_metrics(nexus_url: str, auth: tuple) -> None:
    """Основная функция — получение blobstore и обновление метрик."""
    logging.info("📦 Получаем blobstore из Nexus...")
    blobstores = get_blobstores(nexus_url, auth)
    if not blobstores:
        logging.warning("🚫 Нет данных о blobstore. Метрики не обновлены.")
        return

    update_metrics(blobstores)
