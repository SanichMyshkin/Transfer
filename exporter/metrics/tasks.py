import logging
from typing import Optional
from prometheus_client import Gauge
from metrics.utlis.api import get_from_nexus

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Метрика задач
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


def parse_task_status(last_result: Optional[str]) -> tuple[int, str, str]:
    if last_result == "OK":
        return 1, "✅", "Успешно"
    elif last_result == "FAILED":
        return -1, "❌", "Ошибка"
    elif last_result is None:
        return 2, "⏳", "Не запускалась"
    return -1, "⚠️", f"Неизвестно ({last_result})"


def export_tasks_to_metrics(tasks: list) -> None:
    TASK_INFO.clear()
    for task in tasks:
        task_id = task.get("id", "N/A")
        task_name = task.get("name", "N/A")
        task_type = task.get("type", "N/A")
        last_result = task.get("lastRunResult")
        value, icon, label = parse_task_status(last_result)

        try:
            TASK_INFO.labels(
                id=task_id,
                name=str(task_name),
                type=task_type,
                message=str(task.get("message", "N/A")),
                current_state=task.get("currentState", "N/A"),
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


def fetch_task_metrics(NEXUS_API_URL, endpoint, auth) -> None:
    task_data = get_from_nexus(NEXUS_API_URL, endpoint, auth)
    if not task_data or "items" not in task_data:
        logger.error("❌ Не удалось собрать метрики задач. Пропускаем сбор метрик!")
        return

    logger.info("📥 Получены данные задач Nexus. Начинаем экспорт в метрики...")
    export_tasks_to_metrics(task_data["items"])
