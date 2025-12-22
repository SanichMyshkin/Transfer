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

ACTIVE_WINDOW = "1d"
OUTPUT_FILE = "victoriametrics_repot.xlsx"


def safe_query(prom, q):
    log.info(f"QUERY: {q}")
    try:
        return prom.custom_query(q)
    except PrometheusApiClientException as e:
        log.error(f"Ошибка запроса `{q}`: {e}")
    except Exception as e:
        log.error(f"Ошибка выполнения `{q}`: {e}")
    return None


def count_metric_names_for_job(prom, job):
    q = f'count(count by (__name__) (last_over_time({{job="{job}"}}[{ACTIVE_WINDOW}])))'
    res = safe_query(prom, q)
    if not res:
        return 0
    return int(res[0]["value"][1])


def count_time_series_for_job(prom, job):
    q = f'count(last_over_time({{job="{job}"}}[{ACTIVE_WINDOW}]))'
    res = safe_query(prom, q)
    if not res:
        return 0
    return int(res[0]["value"][1])


def get_instances_for_job(prom, job):
    q = f'count by (instance) (last_over_time({{job="{job}"}}[{ACTIVE_WINDOW}]))'
    res = safe_query(prom, q)
    if not res:
        return []
    return [row["metric"].get("instance", "<none>") for row in res]


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
        log.info("Подключение установлено.")
    except Exception as e:
        log.error(f"Не удалось подключиться к VM: {e}")
        sys.exit(1)

    log.info("Получение списка job...")
    try:
        jobs = sorted(prom.get_label_values("job"))
        log.info(f"Найдено job: {len(jobs)}")
    except Exception as e:
        log.error(f"Ошибка получения job: {e}")
        jobs = []

    workbook = xlsxwriter.Workbook(OUTPUT_FILE)

    sheet_jobs = workbook.add_worksheet("job_metrics")
    headers_jobs = ["job", "metric_names", "time_series", "instances_count", "instances_list"]
    for col, name in enumerate(headers_jobs):
        sheet_jobs.write(0, col, name)

    row = 1
    log.info("Начинаю обработку job...")

    for job in jobs:
        log.info(f"[JOB] {job}")

        metric_names = count_metric_names_for_job(prom, job)
        time_series = count_time_series_for_job(prom, job)
        instances = get_instances_for_job(prom, job)

        sheet_jobs.write(row, 0, job)
        sheet_jobs.write_number(row, 1, metric_names)
        sheet_jobs.write_number(row, 2, time_series)
        sheet_jobs.write_number(row, 3, len(instances))
        sheet_jobs.write(row, 4, ", ".join(sorted(instances)))
        row += 1

    log.info("Получение списка team...")
    try:
        teams = prom.get_label_values("team")
        log.info(f"Найдено team: {len(teams)}")
    except Exception as e:
        log.error(f"Ошибка получения team: {e}")
        teams = []

    groups = defaultdict(lambda: {"metric_names": set(), "instances": set(), "time_series": 0})

    log.info("Обработка team/service_id...")

    for team in sorted(teams):
        if not team:
            continue

        log.info(f"[TEAM] {team}")

        q_svc = f'count by (service_id) (last_over_time({{team="{team}", service_id!=""}}[{ACTIVE_WINDOW}]))'
        svc_rows = safe_query(prom, q_svc)
        service_ids = set()
        if svc_rows:
            for r in svc_rows:
                sid = r["metric"].get("service_id")
                if sid:
                    service_ids.add(sid)

        q_total = f'count(last_over_time({{team="{team}"}}[{ACTIVE_WINDOW}]))'
        total_res = safe_query(prom, q_total)
        total = int(total_res[0]["value"][1]) if total_res else 0

        q_with_svc = f'count(last_over_time({{team="{team}", service_id!=""}}[{ACTIVE_WINDOW}]))'
        with_svc_res = safe_query(prom, q_with_svc)
        with_svc = int(with_svc_res[0]["value"][1]) if with_svc_res else 0

        q_no_svc = f'count(last_over_time({{team="{team}", service_id=""}}[{ACTIVE_WINDOW}]))'
        no_svc_res = safe_query(prom, q_no_svc)
        no_svc = int(no_svc_res[0]["value"][1]) if no_svc_res else 0

        log.info(f"[TEAM] {team}: total={total}, with_service_id={with_svc}, no_service_id={no_svc}")

        for service_id in sorted(service_ids):
            log.info(f"[TEAM-SVC] {team}/{service_id}")

            q_series = f'count(last_over_time({{team="{team}", service_id="{service_id}"}}[{ACTIVE_WINDOW}]))'
            res_series = safe_query(prom, q_series)
            time_series_count = int(res_series[0]["value"][1]) if res_series else 0

            q_instances = f'count by (instance) (last_over_time({{team="{team}", service_id="{service_id}"}}[{ACTIVE_WINDOW}]))'
            inst_rows = safe_query(prom, q_instances)
            inst_list = {r["metric"].get("instance", "<none>") for r in inst_rows} if inst_rows else set()

            q_metrics = f'count by (__name__) (last_over_time({{team="{team}", service_id="{service_id}"}}[{ACTIVE_WINDOW}]))'
            mn_rows = safe_query(prom, q_metrics)
            metric_names = {r["metric"].get("__name__", "<noname>") for r in mn_rows} if mn_rows else set()

            key = f"{team}-{service_id}"
            g = groups[key]
            g["metric_names"].update(metric_names)
            g["instances"].update(inst_list)
            g["time_series"] += time_series_count

        if no_svc > 0:
            log.info(f"[TEAM-<none>] {team} — time_series без service_id: {no_svc}")

            q_instances_ns = f'count by (instance) (last_over_time({{team="{team}", service_id=""}}[{ACTIVE_WINDOW}]))'
            inst_ns_rows = safe_query(prom, q_instances_ns)
            inst_ns = {r["metric"].get("instance", "<none>") for r in inst_ns_rows} if inst_ns_rows else set()

            q_metrics_ns = f'count by (__name__) (last_over_time({{team="{team}", service_id=""}}[{ACTIVE_WINDOW}]))'
            mn_ns_rows = safe_query(prom, q_metrics_ns)
            metric_ns = {r["metric"].get("__name__", "<noname>") for r in mn_ns_rows} if mn_ns_rows else set()

            key = f"{team}-<none>"
            g = groups[key]
            g["metric_names"].update(metric_ns)
            g["instances"].update(inst_ns)
            g["time_series"] += no_svc

    log.info("Обработка service_id без team...")

    q_svc_no_team = f'count by (service_id) (last_over_time({{team="", service_id!=""}}[{ACTIVE_WINDOW}]))'
    svc_no_team_rows = safe_query(prom, q_svc_no_team)
    svc_no_team_ids = {r["metric"].get("service_id") for r in svc_no_team_rows} if svc_no_team_rows else set()

    for service_id in sorted(svc_no_team_ids):
        if not service_id:
            continue

        log.info(f"[<none>-SVC] {service_id}")

        q_series = f'count(last_over_time({{team="", service_id="{service_id}"}}[{ACTIVE_WINDOW}]))'
        res_series = safe_query(prom, q_series)
        time_series_count = int(res_series[0]["value"][1]) if res_series else 0

        q_instances = f'count by (instance) (last_over_time({{team="", service_id="{service_id}"}}[{ACTIVE_WINDOW}]))'
        inst_rows = safe_query(prom, q_instances)
        inst_list = {r["metric"].get("instance", "<none>") for r in inst_rows} if inst_rows else set()

        q_metrics = f'count by (__name__) (last_over_time({{team="", service_id="{service_id}"}}[{ACTIVE_WINDOW}]))'
        mn_rows = safe_query(prom, q_metrics)
        metric_names = {r["metric"].get("__name__", "<noname>") for r in mn_rows} if mn_rows else set()

        key = f"<none>-{service_id}"
        g = groups[key]
        g["metric_names"].update(metric_names)
        g["instances"].update(inst_list)
        g["time_series"] += time_series_count

    log.info("Обработка полностью неразмеченных...")

    q_unlabeled = f'count(last_over_time({{team="", service_id=""}}[{ACTIVE_WINDOW}]))'
    unlabeled_res = safe_query(prom, q_unlabeled)
    unlabeled_series = int(unlabeled_res[0]["value"][1]) if unlabeled_res else 0

    if unlabeled_series > 0:
        log.info(f"[unlabeled] time_series: {unlabeled_series}")

        q_inst_unl = f'count by (instance) (last_over_time({{team="", service_id=""}}[{ACTIVE_WINDOW}]))'
        inst_unl_rows = safe_query(prom, q_inst_unl)
        inst_unl = {r["metric"].get("instance", "<none>") for r in inst_unl_rows} if inst_unl_rows else set()

        q_mn_unl = f'count by (__name__) (last_over_time({{team="", service_id=""}}[{ACTIVE_WINDOW}]))'
        mn_unl_rows = safe_query(prom, q_mn_unl)
        mn_unl = {r["metric"].get("__name__", "<noname>") for r in mn_unl_rows} if mn_unl_rows else set()

        g = groups["unlabeled"]
        g["metric_names"].update(mn_unl)
        g["instances"].update(inst_unl)
        g["time_series"] += unlabeled_series

    log.info("Запись листа team_service_metrics...")

    sheet_groups = workbook.add_worksheet("team_service_metrics")
    headers_groups = ["team_service", "metric_names", "time_series", "instances_count", "instances_list"]
    for col, name in enumerate(headers_groups):
        sheet_groups.write(0, col, name)

    row = 1
    for group_name in sorted(groups.keys()):
        data = groups[group_name]
        sheet_groups.write(row, 0, group_name)
        sheet_groups.write_number(row, 1, len(data["metric_names"]))
        sheet_groups.write_number(row, 2, data["time_series"])
        sheet_groups.write_number(row, 3, len(data["instances"]))
        sheet_groups.write(row, 4, ", ".join(sorted(data["instances"])))
        row += 1

    log.info("Формирование summary...")

    sheet_summary = workbook.add_worksheet("summary")
    sheet_summary.write(0, 0, "metric")
    sheet_summary.write(0, 1, "value")

    all_metric_names = set()
    all_instances = set()
    all_teams = set()
    all_service_ids = set()
    total_time_series_sum = 0

    for job in jobs:
        q = f'count by (__name__) (last_over_time({{job="{job}"}}[{ACTIVE_WINDOW}]))'
        res = safe_query(prom, q)
        if res:
            for r in res:
                mname = r["metric"].get("__name__")
                if mname:
                    all_metric_names.add(mname)
                total_time_series_sum += int(r["value"][1])

        instances = get_instances_for_job(prom, job)
        all_instances.update(instances)

    for group_name, data in groups.items():
        all_instances.update(data["instances"])
        all_metric_names.update(data["metric_names"])

        if "-" in group_name:
            team, svc = group_name.split("-", 1)
            if team not in ("<none>", "unlabeled", ""):
                all_teams.add(team)
            if svc not in ("<none>", "unlabeled", ""):
                all_service_ids.add(svc)

    summary_data = {
        "total_jobs": len(jobs),
        "total_metric_names": len(all_metric_names),
        "total_time_series": total_time_series_sum,
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

    log.info(f"Сохранение файла {OUTPUT_FILE}...")
    workbook.close()
    log.info(f"✔ Готово. Файл {OUTPUT_FILE} создан.")


if __name__ == "__main__":
    main()
