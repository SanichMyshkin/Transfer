import time
import logging

from config import get_auth
from config import NEXUS_API_URL, LAUNCH_INTERVAL, REPO_METRICS_INTERVAL

from prometheus_client import start_http_server


from metrics.repo_status import fetch_repositories_metrics
from metrics.repo_size import fetch_repository_metrics
from metrics.blobs_size import fetch_blob_metrics
from metrics.docker_tags import fetch_docker_tags_metrics
from metrics.tasks import fetch_task_metrics
from metrics.docker_ports import fetch_docker_ports_metrics

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(module)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    start_http_server(8000)
    auth = get_auth()

    logger.info("Метрики VictoriaMetrics доступны на :8000")

    # Запускаем сбор метрик репозиториев сразу
    logger.info("Первичный запуск сбора статуса репозиториев типа Proxy...")
    fetch_repositories_metrics(NEXUS_API_URL, auth)
    last_repo_metrics_time = time.time()

    while True:
        current_time = time.time()
        
        if current_time - last_repo_metrics_time >= REPO_METRICS_INTERVAL:
            logger.info("Периодический запуск сбора статуса репозиториев типа Proxy...")
            fetch_repositories_metrics(NEXUS_API_URL, auth)
            last_repo_metrics_time = current_time

        logger.info("Запуск сбора размера блобов...")
        fetch_blob_metrics(NEXUS_API_URL, auth)

        logger.info("Запуск сбора размера репозиториев и наличие задач очистки...")
        fetch_repository_metrics()

        logger.info("Запуск сбора задач...")
        fetch_task_metrics(NEXUS_API_URL, "tasks", auth)

        logger.info("Запуск сбора Docker тегов...")
        fetch_docker_tags_metrics()

        logger.info("Запуск сбора Docker портов...")
        fetch_docker_ports_metrics()

        time.sleep(LAUNCH_INTERVAL)



if __name__ == "__main__":
    main()
    #task = get_jobs_data()
    #for i in task:
    #    print(i)
    #    print('-'*40)