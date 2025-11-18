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
    log.info(f"QUERY: {q}")
    try:
        return prom.custom_query(q)
    except Exception as e:
        log.error(f"ERROR `{q}`: {e}")
    return None


def get_job_instances(prom, job):
    q = f'count by (instance) ({{job="{job}"}})'
    res = safe_query(prom, q)
    if not res:
        return []
    return [row["metric"].get("instance", "<none>") for row in res]


def count_metric_names_for_job(prom, job):
    q = f'count(count by (__name__) ({{job="{job}"}}))'
    res = safe_query(prom, q)
    if not res:
        return 0
    return int(res[0]["value"][1])


def count_series_for_job(prom, job):
    q = f'count({{job="{job}"}})'
    res = safe_query(prom, q)
    if not res:
        return 0
    return int(res[0]["value"][1])


def main():
    load_dotenv()
    vm_url = os.getenv("VM_URL")

    if not vm_url:
        log.error("VM_URL отсутствует")
        sys.exit(1)

    log.info(f"Подключение: {vm_url}")
    try:
        prom = PrometheusConnect(url=vm_url, disable_ssl=True)
    except Exception as e:
        log.error(f"Подключение провалено: {e}")
        sys.exit(1)

    log.info("Загрузка списка job...")
    try:
        jobs = sorted(prom.get_label_values("job"))
    except:
        jobs = []

    workbook = xlsxwriter.Workbook("job_metrics.xlsx")

    sheet_jobs = workbook.add_worksheet("job_metrics")
    headers_jobs = ["job", "metric_names", "series", "instances_count", "instances_list"]
    for col, name in enumerate(headers_jobs):
        sheet_jobs.write(0, col, name)

    row = 1
    for job in jobs:
        log.info(f"[JOB] {job}")
        metric_names = count_metric_names_for_job(prom, job)
        series = count_series_for_job(prom, job)
        instances = get_job_instances(prom, job)

        sheet_jobs.write(row, 0, job)
        sheet_jobs.write_number(row, 1, metric_names)
        sheet_jobs.write_number(row, 2, series)
        sheet_jobs.write_number(row, 3, len(instances))
        sheet_jobs.write(row, 4, ", ".join(sorted(instances)))
        row += 1

    log.info("Получение team...")
    try:
        teams = prom.get_label_values("team")
    except:
        teams = []

    groups = defaultdict(lambda: {"metric_names": set(), "instances": set(), "series": 0})

    log.info("Обработка team/service_id...")

    for team in sorted(teams):
        if not team:
            continue

        log.info(f"[TEAM] {team}")

        q_svc = f'count by (service_id) ({{team="{team}"}})'
        svc_rows = safe_query(prom, q_svc)
        svc_ids = set()

        if svc_rows:
            for r in svc_rows:
                sid = r["metric"].get("service_id")
                if sid:
                    svc_ids.add(sid)

        q_total = f'count({{team="{team}"}})'
        q_total_res = safe_query(prom, q_total)
        total = int(q_total_res[0]["value"][1]) if q_total_res else 0

        q_has_svc = f'count({{team="{team}", service_id!=""}})'
        q_has_svc_res = safe_query(prom, q_has_svc)
        with_svc = int(q_has_svc_res[0]["value"][1]) if q_has_svc_res else 0

        no_svc = max(total - with_svc, 0)

        for service_id in sorted(svc_ids):
            log.info(f"[TEAM-SVC] {team}/{service_id}")

            q_series = f'count({{team="{team}", service_id="{service_id}"}})'
            res_series = safe_query(prom, q_series)
            series_count = int(res_series[0]["value"][1]) if res_series else 0

            q_instances = f'count by (instance) ({{team="{team}", service_id="{service_id}"}})'
            inst_rows = safe_query(prom, q_instances)
            inst_list = set()
            if inst_rows:
                for r in inst_rows:
                    inst_list.add(r["metric"].get("instance", "<none>"))

            q_metrics = f'count by (__name__) ({{team="{team}", service_id="{service_id}"}})'
            mn_rows = safe_query(prom, q_metrics)
            metric_names = set()
            if mn_rows:
                for r in mn_rows:
                    metric_names.add(r["metric"].get("__name__", "<noname>"))

            g = groups[f"{team}-{service_id}"]
            g["metric_names"].update(metric_names)
            g["instances"].update(inst_list)
            g["series"] += series_count

        if no_svc > 0:
            log.info(f"[TEAM-<none>] {team} — {no_svc} series")

            q_instances = f'count by (instance) ({{team="{team}", service_id=""}})'
            inst_rows = safe_query(prom, q_instances)
            inst_list = set()
            if inst_rows:
                for r in inst_rows:
                    inst_list.add(r["metric"].get("instance", "<none>"))

            q_metrics = f'count by (__name__) ({{team="{team}", service_id=""}})'
            mn_rows = safe_query(prom, q_metrics)
            metric_names = set()
            if mn_rows:
                for r in mn_rows:
                    metric_names.add(r["metric"].get("__name__", "<noname>"))

            g = groups[f"{team}-<none>"]
            g["metric_names"].update(metric_names)
            g["instances"].update(inst_list)
            g["series"] += no_svc

    log.info("Поиск метрик без team и без service_id...")

    q_unlabeled = 'count({team="", service_id=""})'
    unl_res = safe_query(prom, q_unlabeled)
    unlabeled_series = int(unl_res[0]["value"][1]) if unl_res else 0

    if unlabeled_series > 0:
        log.info(f"[UNLABELED] {unlabeled_series} series")

        q_instances = 'count by (instance) ({team="", service_id=""})'
        inst_rows = safe_query(prom, q_instances)
        inst_list = set()
        if inst_rows:
            for r in inst_rows:
                inst_list.add(r["metric"].get("instance", "<none>"))

        q_metrics = 'count by (__name__) ({team="", service_id=""})'
        mn_rows = safe_query(prom, q_metrics)
        metric_names = set()
        if mn_rows:
            for r in mn_rows:
                metric_names.add(r["metric"].get("__name__", "<noname>"))

        g = groups["unlabeled"]
        g["metric_names"].update(metric_names)
        g["instances"].update(inst_list)
        g["series"] += unlabeled_series

    sheet_groups = workbook.add_worksheet("team_service_metrics")
    headers = ["team_service", "metric_names", "series", "instances_count", "instances_list"]
    for col, name in enumerate(headers):
        sheet_groups.write(0, col, name)

    row = 1
    for name in sorted(groups.keys()):
        g = groups[name]
        sheet_groups.write(row, 0, name)
        sheet_groups.write_number(row, 1, len(g["metric_names"]))
        sheet_groups.write_number(row, 2, g["series"])
        sheet_groups.write_number(row, 3, len(g["instances"]))
        sheet_groups.write(row, 4, ", ".join(sorted(g["instances"])))
        row += 1

    sheet_summary = workbook.add_worksheet("summary")
    sheet_summary.write(0, 0, "metric")
    sheet_summary.write(0, 1, "value")

    summary = {
        "groups_count": len(groups),
    }

    row = 1
    for k, v in summary.items():
        sheet_summary.write(row, 0, k)
        sheet_summary.write_number(row, 1, v)
        row += 1

    workbook.close()
    log.info("Готово.")


if __name__ == "__main__":
    main()
