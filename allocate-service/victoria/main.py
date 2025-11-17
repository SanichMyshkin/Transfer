import os
import sys
import logging
from dotenv import load_dotenv
from prometheus_api_client import PrometheusConnect, PrometheusApiClientException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


def load_env():
    load_dotenv()
    url = os.getenv("VM_URL")
    if not url:
        log.error("VM_URL отсутствует")
        sys.exit(1)
    return url


def connect(url: str) -> PrometheusConnect:
    try:
        return PrometheusConnect(url=url, disable_ssl=True)
    except Exception as e:
        log.error(f"Подключение невозможно: {e}")
        sys.exit(1)


def safe_query(prom: PrometheusConnect, q: str):
    try:
        return prom.custom_query(q)
    except PrometheusApiClientException as e:
        log.error(f"Ошибка запроса `{q}`: {e}")
    except Exception as e:
        log.error(f"Ошибка `{q}`: {e}")
    return None


def fetch_jobs(prom: PrometheusConnect):
    try:
        return sorted(prom.get_label_values("job"))
    except Exception as e:
        log.error(f"Невозможно получить job: {e}")
        return []


def fetch_metric_names(prom: PrometheusConnect):
    try:
        return prom.all_metrics()
    except Exception as e:
        log.error(f"Невозможно получить metric names: {e}")
        return []


def analyze_job(prom: PrometheusConnect, job: str):
    q_series = f'count({{job="{job}"}})'
    q_instances = f'count(count by (instance) ({{job="{job}"}}))'
    series = safe_query(prom, q_series)
    inst = safe_query(prom, q_instances)
    if not series or not inst:
        return None
    return job, int(series[0]["value"][1]), int(inst[0]["value"][1])


def main():
    vm_url = load_env()
    log.info(f"URL: {vm_url}")
    prom = connect(vm_url)

    metrics = fetch_metric_names(prom)
    log.info(f"Уникальных metric names: {len(metrics)}")

    jobs = fetch_jobs(prom)
    log.info(f"Уникальных job: {len(jobs)}")

    print("\nРаспределение:\n")
    for job in jobs:
        data = analyze_job(prom, job)
        if not data:
            print(f"  • {job:<25} | error")
            continue
        job_name, series_count, instances_count = data
        print(f"  • {job_name:<25} | series = {series_count:<6} | instances = {instances_count}")

    print("\nГотово.\n")


if __name__ == "__main__":
    main()
