import os
import sys
import logging
from dotenv import load_dotenv
from prometheus_api_client import PrometheusConnect, PrometheusApiClientException
import xlsxwriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def safe_query(prom, q):
    try:
        return prom.custom_query(q)
    except Exception as e:
        log.error(f"Ошибка запроса `{q}`: {e}")
        return None


def count_metric_names(prom, job):
    q = f'count(count by (__name__) ({{job="{job}"}}))'
    res = safe_query(prom, q)
    if not res:
        return 0
    return int(res[0]["value"][1])


def count_series(prom, job):
    q = f'count({{job="{job}"}})'
    res = safe_query(prom, q)
    if not res:
        return 0
    return int(res[0]["value"][1])


def get_instances(prom, job):
    q = f'count by (instance) ({{job="{job}"}})'
    res = safe_query(prom, q)
    if not res:
        return []
    return [row["metric"].get("instance", "<none>") for row in res]


def main():
    load_dotenv()
    vm_url = os.getenv("VM_URL")

    if not vm_url:
        log.error("VM_URL отсутствует")
        sys.exit(1)

    try:
        prom = PrometheusConnect(url=vm_url, disable_ssl=True)
    except Exception as e:
        log.error(f"Не удалось подключиться: {e}")
        sys.exit(1)

    try:
        jobs = sorted(prom.get_label_values("job"))
    except Exception as e:
        log.error(f"Ошибка получения job: {e}")
        jobs = []

    workbook = xlsxwriter.Workbook("job_metrics.xlsx")
    sheet = workbook.add_worksheet("job_metrics")

    headers = ["job", "metric_names", "series", "instances_count", "instances_list"]
    for col, name in enumerate(headers):
        sheet.write(0, col, name)

    row = 1

    for job in jobs:
        log.info(f"Обработка job: {job}")

        metric_names = count_metric_names(prom, job)
        series = count_series(prom, job)
        instances = get_instances(prom, job)
        instances_count = len(instances)

        sheet.write(row, 0, job)
        sheet.write_number(row, 1, metric_names)
        sheet.write_number(row, 2, series)
        sheet.write_number(row, 3, instances_count)
        sheet.write(row, 4, ", ".join(instances))

        row += 1

    workbook.close()
    log.info("✔ job_metrics.xlsx создан.")


if __name__ == "__main__":
    main()
