import os
import sys
import logging
from collections import defaultdict

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


def count_metric_names_for_job(prom, job):
    query = f'count(count by (__name__) ({{job="{job}"}}))'
    res = safe_query(prom, query)
    if not res:
        return 0
    return int(res[0]["value"][1])


def count_series_for_job(prom, job):
    query = f'count({{job="{job}"}})'
    res = safe_query(prom, query)
    if not res:
        return 0
    return int(res[0]["value"][1])


def get_instances_for_job(prom, job):
    query = f'count by (instance) ({{job="{job}"}})'
    res = safe_query(prom, query)
    if not res:
        return []
    return [row["metric"].get("instance", "<none>") for row in res]


def build_group_key(team, service_id):
    if team and service_id:
        return f"{team}-{service_id}"
    if team and not service_id:
        return f"{team}-<none>"
    if not team and service_id:
        return f"<none>-{service_id}"
    return "unlabeled"


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
        log.error(f"Не удалось подключиться к VM: {e}")
        sys.exit(1)

    # ---------- Лист 1: статистика по job ----------
    log.info("Получение списка job...")
    try:
        jobs = sorted(prom.get_label_values("job"))
    except Exception as e:
        log.error(f"Ошибка получения job: {e}")
        jobs = []

    log.info(f"Найдено job: {len(jobs)}")

    workbook = xlsxwriter.Workbook("job_metrics.xlsx")

    sheet_jobs = workbook.add_worksheet("job_metrics")
    headers_jobs = ["job", "metric_names", "series", "instances_count", "instances_list"]
    for col, name in enumerate(headers_jobs):
        sheet_jobs.write(0, col, name)

    row = 1
    log.info("Начинаю расчёт статистики по job...")

    for job in jobs:
        log.info(f"[job] Обработка job: {job}")

        metric_names = count_metric_names_for_job(prom, job)
        series = count_series_for_job(prom, job)
        instances = get_instances_for_job(prom, job)
        instances_count = len(instances)

        sheet_jobs.write(row, 0, job)
        sheet_jobs.write_number(row, 1, metric_names)
        sheet_jobs.write_number(row, 2, series)
        sheet_jobs.write_number(row, 3, instances_count)
        sheet_jobs.write(row, 4, ", ".join(sorted(instances)))

        row += 1

    # ---------- Лист 2: статистика по team-service_id ----------
    log.info("Запрос агрегированных данных по team/service_id/instance/metric...")

    # Один общий запрос, без дублей: всё сразу
    query_group = 'count by (team, service_id, instance, __name__) ({__name__!=""})'
    group_rows = safe_query(prom, query_group)

    groups = defaultdict(lambda: {"metric_names": set(), "instances": set(), "series": 0})

    if group_rows:
        for row_data in group_rows:
            metric_labels = row_data.get("metric", {})
            value = int(row_data["value"][1])

            team = metric_labels.get("team")
            service_id = metric_labels.get("service_id")
            instance = metric_labels.get("instance", "<none>")
            metric_name = metric_labels.get("__name__", "<noname>")

            group = build_group_key(team, service_id)
            g = groups[group]
            g["metric_names"].add(metric_name)
            g["instances"].add(instance)
            g["series"] += value

    sheet_groups = workbook.add_worksheet("team_service_metrics")
    headers_groups = [
        "team_service",
        "metric_names",
        "series",
        "instances_count",
        "instances_list",
    ]
    for col, name in enumerate(headers_groups):
        sheet_groups.write(0, col, name)

    log.info("Запись статистики по team-service_id в Excel...")

    row = 1
    for group_name in sorted(groups.keys()):
        data = groups[group_name]
        metric_names_count = len(data["metric_names"])
        series_count = data["series"]
        instances_list = sorted(data["instances"])
        instances_count = len(instances_list)

        sheet_groups.write(row, 0, group_name)
        sheet_groups.write_number(row, 1, metric_names_count)
        sheet_groups.write_number(row, 2, series_count)
        sheet_groups.write_number(row, 3, instances_count)
        sheet_groups.write(row, 4, ", ".join(instances_list))

        row += 1

    log.info("Сохранение файла job_metrics.xlsx...")
    workbook.close()
    log.info("✔ Готово. Файл job_metrics.xlsx создан.")


if __name__ == "__main__":
    main()
