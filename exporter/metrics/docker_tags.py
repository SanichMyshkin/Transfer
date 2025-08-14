from prometheus_client import Gauge
from metrics.utils.api import build_nexus_url
from database.docker_tags_query import fetch_docker_tags_data  # новый путь
from common.logs import logging


# Метрика Prometheus
docker_tags_gauge = Gauge(
    "docker_image_tags_info",
    "Информация о Docker-образах и их тегах",
    ["image_name", "tags", "repository", "format", "blob", "nexus_url_path"],
)


def process_docker_result(result: list) -> list:
    tags_list = []

    for row in result:
        image, tag, repo, repo_format, blob_name = row

        found = False
        for entry in tags_list:
            if (
                entry["image"] == image
                and entry["repoName"] == repo
                and entry["repoFormat"] == repo_format
                and entry["blobName"] == blob_name
            ):
                if tag not in entry["tags"]:
                    entry["tags"].append(tag)
                found = True
                break

        if not found:
            tags_list.append(
                {
                    "image": image,
                    "tags": [tag],
                    "repoName": repo,
                    "repoFormat": repo_format,
                    "blobName": blob_name,
                }
            )

    return tags_list


def fetch_docker_tags_metrics() -> None:
    try:
        result = fetch_docker_tags_data()
    except Exception as e:
        logging.error(f"❌ Ошибка при получении данных из БД для Docker-образов: {e}")
        logging.warning(
            "⚠️ Возможно, база данных недоступна или Nexus не работает. Сбор метрик пропущен."
        )
        return

    if not result:
        logging.warning(
            "❌ База данных вернула 0 строк по Docker-образам. "
            "Возможно, Nexus не отвечает, база пуста или репозиториев нет. Метрики не обновлены."
        )
        return

    logging.info(f"📥 Получено {len(result)} строк из базы данных.")
    grouped = process_docker_result(result)

    docker_tags_gauge.clear()

    for entry in grouped:
        image = entry["image"]
        tags = sorted(entry["tags"])
        repo = entry["repoName"]
        repo_format = entry["repoFormat"]
        blob = entry["blobName"]

        tag_str = "; ".join(tags)
        tag_count = len(tags)

        logging.info(
            f"🐳 Образ: {image} | 📦 Репозиторий: {repo} | 🏷️ Теги ({tag_count}): {tag_str}"
        )

        docker_tags_gauge.labels(
            image_name=image,
            tags=tag_str,
            repository=repo,
            format=repo_format,
            blob=blob,
            nexus_url_path=build_nexus_url(repo, image, encoding=False),
        ).set(tag_count)

    logging.info(f"✅ Метрики обновлены для {len(grouped)} Docker-образов.")
