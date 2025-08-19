import time
from common.logs import logging

from common.config import get_auth
from common.config import NEXUS_API_URL, LAUNCH_INTERVAL, REPO_METRICS_INTERVAL

from prometheus_client import start_http_server


from metrics.repo_status import fetch_repositories_metrics
from metrics.repo_size import fetch_repository_metrics
from metrics.blobs_size import fetch_blob_metrics
from metrics.docker_tags import fetch_docker_tags_metrics
from metrics.tasks import fetch_task_metrics, fetch_all_blob_and_repo_metrics
from metrics.docker_ports import fetch_docker_ports_metrics
from metrics.cleanup_policy import fetch_cleanup_policy_usage
from metrics.certificates_expired import fetch_cert_lifetime_metrics


def main():
    start_http_server(8000)
    auth = get_auth()

    logging.info("Метрики VictoriaMetrics доступны на :8000")

    # Запускаем сбор метрик репозиториев сразу
    logging.info("Первичный запуск сбора статуса репозиториев типа Proxy...")
    fetch_repositories_metrics(NEXUS_API_URL, auth)
    last_repo_metrics_time = time.time()

    while True:
        current_time = time.time()

        if current_time - last_repo_metrics_time >= REPO_METRICS_INTERVAL:
            logging.info(
                "Периодический запуск сбора статуса репозиториев типа Proxy..."
            )
            fetch_repositories_metrics(NEXUS_API_URL, auth)
            last_repo_metrics_time = current_time

        logging.info("Запуск сбора размера блобов...")
        fetch_blob_metrics(NEXUS_API_URL, auth)

        logging.info("Запуск сбора размера репозиториев и наличие задач очистки...")
        fetch_repository_metrics()

        logging.info("Запуск сбора задач...")
        fetch_task_metrics(NEXUS_API_URL, auth)

        logging.info("Запуск сбора повисших задач...")
        fetch_all_blob_and_repo_metrics(NEXUS_API_URL, auth)

        logging.info("Запуск сбора Docker тегов...")
        fetch_docker_tags_metrics()

        logging.info("Запуск сбора Docker портов...")
        fetch_docker_ports_metrics()

        logging.info("Запуск сбора НЕ используемых политик...")
        fetch_cleanup_policy_usage(NEXUS_API_URL, auth)

        logging.info("Запуск сбора сертификатов...")
        fetch_cert_lifetime_metrics(NEXUS_API_URL, get_auth())

        time.sleep(LAUNCH_INTERVAL)


if __name__ == "__main__":
    main()
