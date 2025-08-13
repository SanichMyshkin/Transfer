import logging
from typing import Optional
from prometheus_client import Gauge
from metrics.utils.api import get_from_nexus
from database.jobs_query import get_jobs_data  # твоя функция

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
)
logger = logging.getLogger(__name__)

# --- Метрики ---
TASK_INFO = Gauge(
    "nexus_task_info",
    "Raw info about all Nexus tasks",
    [
        "id",
        "name",
        "type",
        "message",
        "current_state",
        "last_run_result",
        "next_run",
        "last_run",
    ],
)

TASK_MATCH_INFO = Gauge(
    "nexus_task_match_info",
    "Filtered tasks with matching blobstore or repository",
    ["task_id", "task_name", "type", "typeName", "match_type", "match_value"],
)


# --- Вспомогательные функции ---
def parse_task_status(last_result: Optional[str]) -> tuple[int, str, str]:
    if last_result == "OK":
        return 1, "✅", "Успешно"
    elif last_result == "FAILED":
        return -1, "❌", "Ошибка"
    elif last_result is None:
        return 2, "⏳", "Не запускалась"
    return -1, "⚠️", f"Неизвестно ({last_result})"


def fetch_all_from_nexus(NEXUS_API_URL: str, endpoint: str, auth) -> list:
    """Получает все страницы данных из Nexus API."""
    results = []
    continuation_token = None

    while True:
        url = endpoint
        if continuation_token:
            url = f"{endpoint}?continuationToken={continuation_token}"

        data = get_from_nexus(NEXUS_API_URL, url, auth)
        if not data:
            break

        if isinstance(data, list):
            results.extend(data)
            break

        if isinstance(data, dict):
            items = data.get("items", [])
            if isinstance(items, list):
                results.extend(items)

            continuation_token = data.get("continuationToken")
            if not continuation_token:
                break
        else:
            break

    return results


def export_tasks_to_metrics(tasks: list) -> None:
    """Экспорт полной информации о задачах."""
    TASK_INFO.clear()
    for task in tasks:
        task_id = task.get("id", task.get(".id", "N/A"))
        task_name = task.get("name", task.get(".name", "N/A"))
        task_type = task.get("type", task.get(".typeId", "N/A"))
        last_result = task.get("lastRunResult")
        value, icon, label = parse_task_status(last_result)

        try:
            TASK_INFO.labels(
                id=str(task_id),
                name=str(task_name),
                type=str(task_type),
                message=str(task.get("message", "N/A")),
                current_state=str(task.get("currentState", "N/A")),
                last_run_result=last_result or "null",
                next_run=task.get("nextRun") or "null",
                last_run=task.get("lastRun") or "null",
            ).set(value)

            logger.info(f"📊 [{icon}] Задача '{task_name}' ({task_type}): {label}")
        except Exception as e:
            logger.warning(
                f"⚠️ Ошибка при экспорте метрик для задачи {task_id}: {e}", exc_info=True
            )

    logger.info("✅ Экспорт метрик задач завершён.")


def export_blob_repo_metrics(tasks: list, blobs: list, repos: list) -> None:
    """Экспорт метрик только с blobstore/repo."""
    TASK_MATCH_INFO.clear()

    for task in tasks:
        tid = task.get("id", task.get(".id", "N/A"))
        name = task.get("name", task.get(".name", "N/A"))
        task_type = task.get("type", task.get(".typeId", "N/A"))
        type_name = task.get("typeName", task.get(".typeName", "N/A"))

        blob = task.get("blobstoreName")
        repo = task.get("repositoryName")

        if blob:
            exists = 1 if blob.lower() in blobs else 0
            match_status = "✅" if exists else "❌"
            logger.info(
                f"📦 [{match_status}] Задача '{name}' ({task_type}) [blobstore: {blob}]"
            )

            TASK_MATCH_INFO.labels(
                task_id=str(tid),
                task_name=str(name),
                type=str(task_type),
                typeName=str(type_name),
                match_type="blobstore",
                match_value=blob,
            ).set(exists)

        if repo:
            exists = 1 if repo.lower() in repos else 0
            match_status = "✅" if exists else "❌"
            logger.info(
                f"📦 [{match_status}] Задача '{name}' ({task_type}) [repository: {repo}]"
            )

            TASK_MATCH_INFO.labels(
                task_id=str(tid),
                task_name=str(name),
                type=str(task_type),
                typeName=str(type_name),
                match_type="repository",
                match_value=repo,
            ).set(exists)

    logger.info(f"✅ Экспорт blob/repo метрик завершён. Обработано задач: {len(tasks)}")


# --- Основные функции ---
def fetch_task_metrics(NEXUS_API_URL, auth) -> None:
    """Сбор метрики всех задач Nexus."""
    task_data = fetch_all_from_nexus(NEXUS_API_URL, "tasks", auth)
    if not task_data:
        logger.error("❌ Не удалось собрать метрики задач. Пропускаем сбор метрик!")
        return

    logger.info(
        f"📥 Получены данные задач Nexus: {len(task_data)} записей. Начинаем экспорт..."
    )
    export_tasks_to_metrics(task_data)


def fetch_all_blob_and_repo_metrics(NEXUS_API_URL, auth) -> None:
    """Сбор метрик с проверкой blobstore и repository."""
    logger.info("📥 Загружаем список blobstore и repository из Nexus...")

    blobs_data = fetch_all_from_nexus(NEXUS_API_URL, "blobstores", auth)
    repos_data = fetch_all_from_nexus(NEXUS_API_URL, "repositories", auth)

    blobs = [
        b.get("name", "").lower()
        for b in blobs_data
        if isinstance(b, dict) and "name" in b
    ]
    repos = [
        r.get("name", "").lower()
        for r in repos_data
        if isinstance(r, dict) and "name" in r
    ]

    logger.info(f"📦 Найдено blobstores: {len(blobs)}, repositories: {len(repos)}")

    tasks = get_jobs_data()
    filtered_tasks = [
        t for t in tasks if t.get("blobstoreName") or t.get("repositoryName")
    ]

    logger.info(f"🔍 Отфильтровано задач для проверки blob/repo: {len(filtered_tasks)}")
    export_blob_repo_metrics(filtered_tasks, blobs, repos)
