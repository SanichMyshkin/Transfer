import os
import sys
import logging
from dotenv import load_dotenv
from prometheus_api_client import PrometheusConnect, PrometheusApiClientException
import xlsxwriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


def safe_query(prom, q):
    try:
        return prom.custom_query(q)
    except PrometheusApiClientException as e:
        log.error(f"Ошибка запроса `{q}`: {e}")
    except Exception as e:
        log.error(f"Ошибка выполнения `{q}`: {e}")
    return None


def get_all_metric_names(prom):
    res = safe_query(prom, 'label_values(__name__)')
    if not res:
        return []
    return [row["value"][1] for row in res]


def count_metric_names(prom, job, all_metric_names):
    count_names = 0
    for metric_name in all_metric_names:
        q = f'count({{__name__="{metric_name}", job="{job}"}})'
        r = safe_query(prom, q)
        if r and int(r[0]["value"][1]) > 0:
            count_names += 1
    return count_names


def count_series(prom, job):
    q = f'count({{job="{job}"}})'
    res = safe_query(prom, q)
    if not res:
        return 0
    return int(res[0]["value"][1])


def count_instances(prom, job):
    q = f'count(count by (instance) ({{job="{job}"}}))'
    res = safe_query(prom, q)
    if not res:
        return 0
    return int(res[0]["value"][1])


def main():
    log.info("Загрузка переменных окружения...")
    load_dotenv()
    vm_url = os.getenv("VM_URL")

    if not vm_url:
        log.error("VM_URL отсутствует")
        sys.exit(1)

    log.info(f"Подключение к VictoriaMetrics: {vm_url}")
    try:
        prom = PrometheusConnect(url=vm_url, disable_ssl=True)
    except Exception as e:
        log.error(f"Не удалось подключиться: {e}")
        sys.exit(1)

    log.info("Получение списка job...")
    try:
        jobs = sorted(prom.get_label_values("job"))
    except Exception as e:
        log.error(f"Ошибка получения job: {e}")
        jobs = []

    log.info(f"Найдено job: {len(jobs)}")

    log.info("Получение всех metric names...")
    all_metric_names = get_all_metric_names(prom)
    log.info(f"Найдено metric names: {len(all_metric_names)}")

    workbook = xlsxwriter.Workbook("job_metrics.xlsx")
    sheet = workbook.add_worksheet("job_metrics")
    headers = ["job", "metric_names", "series", "instances"]
    for col, name in enumerate(headers):
        sheet.write(0, col, name)

    row = 1

    log.info("Начинаю обработку job...\n")

    for job in jobs:
        log.info(f"[{row}] Обработка job: {job}")

        log.info("   → Подсчёт metric_names...")
        metric_names = count_metric_names(prom, job, all_metric_names)

        log.info("   → Подсчёт series...")
        series = count_series(prom, job)

        log.info("   → Подсчёт instances...")
        instances = count_instances(prom, job)

        log.info("   → Запись в Excel...")

        sheet.write(row, 0, job)
        sheet.write_number(row, 1, metric_names)
        sheet.write_number(row, 2, series)
        sheet.write_number(row, 3, instances)

        row += 1

    log.info("Сохранение файла job_metrics.xlsx...")
    workbook.close()
    log.info("✔ Готово. Файл job_metrics.xlsx создан.\n")


if __name__ == "__main__":
    main()
