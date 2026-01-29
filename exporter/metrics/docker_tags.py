from prometheus_client import Gauge

from database.docker_tags_query import fetch_docker_tags_data
from metrics.utils.api import build_nexus_url
from common.logs import logging


docker_tags_count_gauge = Gauge(
    "docker_image_tags_count",
    "Количество тегов у Docker-образа в репозитории",
    ["image_name", "repository", "format", "blob", "nexus_url_path"],
)


def fetch_docker_tags_metrics() -> None:
    try:
        result = fetch_docker_tags_data()
    except Exception as e:
        logging.error(f"❌ Ошибка при получении данных из БД для Docker-образов: {e}")
        logging.warning(
            "⚠️ Метрики по Docker-образам не обновлены (БД недоступна или ошибка запроса)."
        )
        return

    if not result:
        logging.warning(
            "⚠️ База данных вернула 0 строк по Docker-образам. Метрики не обновлены."
        )
        return

    logging.info(f"📥 Получено {len(result)} агрегированных строк из БД.")

    docker_tags_count_gauge.clear()

    for row in result:
        image, repo, repo_format, blob, tag_count = row

        logging.info(
            f"🐳 Образ: {image} | 📦 Репо: {repo} | 🧩 Формат: {repo_format} | 🧱 Blob: {blob} | 🏷️ Тегов: {tag_count}"
        )

        docker_tags_count_gauge.labels(
            image_name=image,
            repository=repo,
            format=repo_format,
            blob=blob,
            nexus_url_path=build_nexus_url(repo, image, encoding=False),
        ).set(tag_count)

    logging.info(f"✅ Метрики обновлены для {len(result)} Docker-образов.")
