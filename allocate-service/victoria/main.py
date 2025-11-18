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


def get_instances_for_job(prom, job):
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
        log.error(f"Не удалось подключиться к VM: {e}")
        sys.exit(1)

    try:
        jobs = sorted(prom.get_label_values("job"))
    except Exception as e:
        log.error(f"Ошибка получения job: {e}")
        jobs = []

    workbook = xlsxwriter.Workbook("job_metrics.xlsx")

    sheet_jobs = workbook.add_worksheet("job_metrics")
    headers_jobs = ["job", "metric_names", "series", "instances_count", "instances_list"]
    for col, name in enumerate(headers_jobs):
        sheet_jobs.write(0, col, name)

    row = 1

    for job in jobs:
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

    try:
        teams = prom.get_label_values("team")
    except Exception as e:
        log.error(f"Ошибка получения team: {e}")
        teams = []

    groups = defaultdict(lambda: {"metric_names": set(), "instances": set(), "series": 0})

    for team in sorted(teams):
        if not team:
            continue

        try:
            service_ids = prom.get_label_values("service_id", match=[f'team="{team}"'])
        except Exception as e:
            log.error(f"Ошибка получения service_id для team={team}: {e}")
            service_ids = []

        for service_id in service_ids:
            if not service_id:
                continue

            q_series = f'count({{team="{team}", service_id="{service_id}"}})'
            res_series = safe_query(prom, q_series)
            series_count = int(res_series[0]["value"][1]) if res_series else 0

            q_instances = f'count by (instance) ({{team="{team}", service_id="{service_id}"}})'
            res_instances = safe_query(prom, q_instances)
            instances = []
            if res_instances:
                for r in res_instances:
                    instances.append(r["metric"].get("instance", "<none>"))

            q_metric_names = f'count by (__name__) ({{team="{team}", service_id="{service_id}"}})'
            res_metric_names = safe_query(prom, q_metric_names)
            metric_names = set()
            if res_metric_names:
                for r in res_metric_names:
                    metric_names.add(r["metric"].get("__name__", "<noname>"))

            group_key = f"{team}-{service_id}"

            groups[group_key]["metric_names"].update(metric_names)
            groups[group_key]["instances"].update(instances)
            groups[group_key]["series"] += series_count

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

    row = 1

    for group_name in sorted(groups.keys()):
        data = groups[group_name]

        sheet_groups.write(row, 0, group_name)
        sheet_groups.write_number(row, 1, len(data["metric_names"]))
        sheet_groups.write_number(row, 2, data["series"])
        sheet_groups.write_number(row, 3, len(data["instances"]))
        sheet_groups.write(row, 4, ", ".join(sorted(data["instances"])))

        row += 1

    sheet_summary = workbook.add_worksheet("summary")
    sheet_summary.write(0, 0, "metric")
    sheet_summary.write(0, 1, "value")

    all_metric_names = set()
    all_instances = set()
    all_teams = set()
    all_service_ids = set()
    total_series_sum = 0

    for job in jobs:
        q = f'count by (__name__) ({{job="{job}"}})'
        res = safe_query(prom, q)
        if res:
            for r in res:
                mname = r["metric"].get("__name__")
                if mname:
                    all_metric_names.add(mname)
                total_series_sum += int(r["value"][1])

        instances = get_instances_for_job(prom, job)
        for inst in instances:
            all_instances.add(inst)

    for group_name, data in groups.items():
        for inst in data["instances"]:
            all_instances.add(inst)
        for mn in data["metric_names"]:
            all_metric_names.add(mn)
        if "-" in group_name:
            team, svc = group_name.split("-", 1)
            if team:
                all_teams.add(team)
            if svc:
                all_service_ids.add(svc)

    summary_data = {
        "total_jobs": len(jobs),
        "total_metric_names": len(all_metric_names),
        "total_series": total_series_sum,
        "unique_instances_count": len(all_instances),
        "unique_teams_count": len(all_teams),
        "unique_service_ids_count": len(all_service_ids),
        "team_service_groups_count": len(groups),
    }

    row_s = 1
    for k, v in summary_data.items():
        sheet_summary.write(row_s, 0, k)
        sheet_summary.write_number(row_s, 1, v)
        row_s += 1

    workbook.close()
    log.info("✔ Готово. Файл job_metrics.xlsx создан.")


if __name__ == "__main__":
    main()
