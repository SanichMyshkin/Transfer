import logging
from prometheus_client import Gauge

from database.ports_query import fetch_docker_ports


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(module)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Метрика Prometheus
docker_repo_port_gauge = Gauge(
    "docker_repository_port_info",
    "Информация о портах и удалённых адресах docker-репозиториев Nexus",
    ["repository_name", "http_port", "remote_url", "repo_type"],
)


def fetch_docker_ports_metrics() -> None:
    try:
        result = fetch_docker_ports()
    except Exception as e:
        logger.error(f"❌ Ошибка при обращении к базе данных docker-репозиториев: {e}")
        logger.warning("⚠️ Метрики не обновлены. Возможно, база данных недоступна или повреждена.")
        return

    if not result:
        logger.warning(
            "🚫 Не получено ни одного docker-репозитория из базы данных. "
            "Скорее всего, Nexus недоступен или база пуста. Пропускаем обновление метрик."
        )
        return

    logger.info(f"✅ Получено {len(result)} docker-репозиториев из базы данных.")
    docker_repo_port_gauge.clear()

    for entry in result:
        repo_name = entry.get("repository_name", "unknown")
        http_port = entry.get("http_port")
        remote_url = entry.get("remote_url", "")

        logger.info(
            f"📦 Репозиторий: {repo_name} | 🌐 Порт: {http_port} | 🔗 Удалённый URL: {remote_url or '—'}"
        )

        docker_repo_port_gauge.labels(
            repository_name=repo_name,
            http_port=str(http_port) if http_port is not None else "None",
            remote_url=remote_url if remote_url else "None",
            repo_type="Proxy" if remote_url else "Hosted",
        ).set(1)

    logger.info("✅ Метрики по портам docker-репозиториев успешно обновлены.")
