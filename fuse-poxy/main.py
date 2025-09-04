import requests
import warnings
import os
import logging
import time
from logging.handlers import TimedRotatingFileHandler
from urllib3.exceptions import InsecureRequestWarning
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()


VICTORIA_METRICS_URL = os.getenv("VICTORIA_METRICS_URL")
NEXUS_URL = os.getenv("NEXUS_URL")
NEXUS_USER = os.getenv("NEXUS_USER")
NEXUS_PASS = os.getenv("NEXUS_PASS")

NODE_INSTANCE = os.getenv("NODE_INSTANCE", None)
NODE_MOUNTPOINT = os.getenv("NODE_MOUNTPOINT", "/")
DISK_USAGE_THRESHOLD = float(os.getenv("DISK_USAGE_THRESHOLD", "80.0"))


warnings.simplefilter("ignore", InsecureRequestWarning)

log_dir = Path(__file__).resolve().parent / "logs"
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "script.log"

logger = logging.getLogger("nexus_cleaner")
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler(
    log_file, when="D", interval=1, backupCount=7, encoding="utf-8"
)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def query_victoria_metrics(promql: str):
    url = f"{VICTORIA_METRICS_URL}/api/v1/query"
    try:
        response = requests.get(url, params={"query": promql}, timeout=10)
        response.raise_for_status()
        return response.json().get("data", {}).get("result", [])
    except Exception as e:
        logger.error(f"Ошибка запроса к VictoriaMetrics: {e}")
        return []


def get_disk_usage_percent(instance: str, mountpoint: str) -> float | None:
    """
    Возвращает % занятого места с node_exporter (через VictoriaMetrics).
    Если не удалось получить метрику – None.
    """
    promql = (
        f"(1 - (node_filesystem_avail_bytes{{instance='{instance}', mountpoint='{mountpoint}', fstype!='rootfs'}} "
        f"/ node_filesystem_size_bytes{{instance='{instance}', mountpoint='{mountpoint}', fstype!='rootfs'}})) * 100"
    )
    results = query_victoria_metrics(promql)
    if not results:
        logger.error(
            f"Не удалось получить метрику использования диска для {instance} {mountpoint}"
        )
        return None

    try:
        value = float(results[0]["value"][1])
        return value
    except (KeyError, ValueError, IndexError) as e:
        logger.error(f"Ошибка разбора результата метрики: {e}")
        return None


def get_heaviest_proxy_repo():
    results = query_victoria_metrics("nexus_repo_size")
    heaviest_repo, blob, max_size = None, None, -1
    for metric in results:
        try:
            if metric["metric"].get("repo_type") == "proxy":
                size = float(metric["value"][1])
                if size > max_size:
                    max_size = size
                    heaviest_repo = metric["metric"].get("repo_name")
                    blob = metric["metric"].get("blob_name")
        except (KeyError, ValueError, IndexError):
            continue
    return (heaviest_repo, blob, max_size) if heaviest_repo else None


def get_repository_format(repo_name: str) -> str:
    url = f"{NEXUS_URL}/repositories"
    try:
        resp = requests.get(
            url, auth=(NEXUS_USER, NEXUS_PASS), timeout=10, verify=False
        )
        resp.raise_for_status()
        for repo in resp.json():
            if repo.get("name") == repo_name:
                return repo.get("format")
    except requests.RequestException as e:
        logger.error(f"Ошибка получения формата репозитория {repo_name}: {e}")
    return None


def purge_repository(repo_name: str):
    repo_format = get_repository_format(repo_name)
    if not repo_format:
        logger.warning(f"Не удалось определить формат репозитория '{repo_name}'")
        return

    logger.info(f"Начинаю очистку репозитория '{repo_name}' (формат: {repo_format})")

    session = requests.Session()
    session.auth = (NEXUS_USER, NEXUS_PASS)
    session.verify = False

    deleted_count = 0
    continuation_token = None

    base_url = (
        f"{NEXUS_URL}/search/assets" if repo_format == "raw" else f"{NEXUS_URL}/search"
    )
    delete_url = (
        f"{NEXUS_URL}/assets" if repo_format == "raw" else f"{NEXUS_URL}/components"
    )

    while True:
        params = {"repository": repo_name}
        if continuation_token:
            params["continuationToken"] = continuation_token
        try:
            resp = session.get(base_url, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.error(f"Ошибка получения списка: {e}")
            break

        items = data.get("items", [])
        if not items:
            break

        for item in items:
            item_id = item.get("id")
            if not item_id:
                continue
            try:
                del_resp = session.delete(f"{delete_url}/{item_id}", timeout=20)
                if del_resp.status_code == 204:
                    deleted_count += 1
                    if repo_format == "raw":
                        logger.info(f"Удалён asset: {item.get('path')}")
                    else:
                        logger.info(
                            f"Удалён компонент: {item.get('name')}:{item.get('version')}"
                        )
                else:
                    logger.warning(f"Ошибка удаления {item_id}: {del_resp.status_code}")
            except requests.RequestException as e:
                logger.error(f"Ошибка при удалении {item_id}: {e}")

        continuation_token = data.get("continuationToken")
        if not continuation_token:
            break

    logger.info(
        f"Очистка репозитория '{repo_name}' завершена. Удалено: {deleted_count} объектов."
    )


def run_nexus_task(task_type: str, blob_name: str):
    """Запускает задачу Nexus (compact или docker.gc) и ждёт её завершения"""
    results = query_victoria_metrics(f'nexus_task_match_info{{type="{task_type}"}}')
    if not results:
        logger.warning(f"Не найдена задача {task_type}")
        return

    task_id, task_name = None, None
    for metric in results:
        labels = metric.get("metric", {})
        mv = labels.get("match_value")
        if labels.get("type") == task_type:
            if mv == "*" or mv == blob_name:
                task_id = labels.get("task_id")
                task_name = labels.get("task_name")
                break

    if not task_id:
        logger.info(f"Нет задачи {task_type}, подходящей для блоба {blob_name}")
        return

    # Проверяем lastRun (скипаем, если меньше 2 часов назад)
    try:
        status_resp = requests.get(
            f"{NEXUS_URL}/tasks/{task_id}",
            auth=(NEXUS_USER, NEXUS_PASS),
            timeout=10,
            verify=False,
        )
        status_resp.raise_for_status()
        task_info = status_resp.json()
        last_run = task_info.get("lastRun")
        if last_run:
            last_run_time = datetime.fromisoformat(last_run.replace("Z", "+00:00"))
            if datetime.utcnow().astimezone() - last_run_time < timedelta(hours=2):
                logger.info(
                    f"Пропускаем задачу {task_name} (task_id={task_id}), "
                    f"запускалась {last_run_time}, менее 2 часов назад."
                )
                return
    except Exception as e:
        logger.error(f"Ошибка проверки последнего запуска {task_id}: {e}")
        return

    logger.info(
        f"Нашёл задачу {task_type}: task_id={task_id}, name={task_name}. Запускаю..."
    )

    try:
        run_resp = requests.post(
            f"{NEXUS_URL}/tasks/{task_id}/run",
            auth=(NEXUS_USER, NEXUS_PASS),
            timeout=10,
            verify=False,
        )
        if run_resp.status_code not in (200, 202, 204):
            logger.error(
                f"Не удалось запустить задачу {task_id}: {run_resp.status_code}"
            )
            return
    except requests.RequestException as e:
        logger.error(f"Ошибка запуска задачи {task_id}: {e}")
        return

    logger.info(f"Задача {task_id} запущена. Проверяю статус...")

    max_checks = 9
    for _ in range(max_checks):
        try:
            status_resp = requests.get(
                f"{NEXUS_URL}/tasks/{task_id}",
                auth=(NEXUS_USER, NEXUS_PASS),
                timeout=10,
                verify=False,
            )
            status_resp.raise_for_status()
            data = status_resp.json()
            state = data.get("currentState")
            last_result = data.get("lastRunResult")
            last_run = data.get("lastRun")

            logger.info(
                f"Статус задачи {task_id}: state={state}, lastRunResult={last_result}, lastRun={last_run}"
            )

            if state == "WAITING":
                if last_result == "OK":
                    logger.info(f"Задача {task_id} ({task_type}) завершена успешно")
                else:
                    logger.error(
                        f"Задача {task_id} ({task_type}) завершилась с ошибкой ({last_result})"
                    )
                return
            elif state == "RUNNING":
                logger.info(f"Задача {task_id} всё ещё выполняется...")

        except requests.RequestException as e:
            logger.error(f"Ошибка проверки статуса задачи {task_id}: {e}")
            return
        time.sleep(20)

    logger.warning(f"Статус задачи {task_id} неизвестен (ждали 3 минуты)")


if __name__ == "__main__":
    # disk_usage = get_disk_usage_percent(NODE_INSTANCE, NODE_MOUNTPOINT)
    disk_usage = 90.0  # заглушка, так как на тесте не настроен нод экспортер
    if disk_usage is None:
        logger.warning("Пропускаем — нет данных по дисковому пространству.")
    else:
        logger.info(
            f"Использование диска на {NODE_INSTANCE}:{NODE_MOUNTPOINT} = {disk_usage:.2f}%"
        )

        if disk_usage >= DISK_USAGE_THRESHOLD:
            result = get_heaviest_proxy_repo()
            if result:
                repo_name, blob, size = result
                logger.info(
                    f"Самый тяжёлый прокси-репозиторий: {repo_name} {blob} ({size} байт)"
                )
                purge_repository(repo_name)
                run_nexus_task("blobstore.compact", blob)

                if get_repository_format(repo_name) == "docker":
                    run_nexus_task("repository.docker.gc", blob)
            else:
                logger.warning(
                    "Не удалось определить самый тяжёлый прокси-репозиторий."
                )
        else:
            logger.info(
                f"Диск занят на {disk_usage:.2f}%, меньше порога {DISK_USAGE_THRESHOLD}%. Чистка не требуется."
            )
