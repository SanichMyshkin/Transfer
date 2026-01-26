import os
import sys
import logging
from collections import defaultdict

import requests
from dotenv import load_dotenv
from prometheus_api_client import PrometheusConnect
import xlsxwriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MAX_LOOKBACK = "1d"
OUTPUT_FILE = "victoriametrics_repot.xlsx"
HTTP_TIMEOUT_SEC = 60


def safe_query(prom, q):
    log.info(f"QUERY: {q}")
    try:
        url = prom.url.rstrip("/") + "/api/v1/query"
        r = requests.get(
            url,
            params={"query": q, "max_lookback": MAX_LOOKBACK},
            verify=False,
            timeout=HTTP_TIMEOUT_SEC,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "success":
            log.error(f"Bad response for `{q}`: {data}")
            return None
        return data.get("data", {}).get("result", [])
    except Exception as e:
        log.error(f"Ошибка выполнения `{q}`: {e}")
        return None


def count_metric_names_for_job(prom, job):
    q = f'count(count by (__name__) ({{job="{job}"}}))'
    res = safe_query(prom, q)
    if not res:
        return 0
    return int(res[0]["value"][1])


def count_time_series_for_job(prom, job):
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
    headers_jobs = [
        "job",
        "metric_names",
        "time_series",
        "instances_count",
        "instances_list",
    ]
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

    groups = defaultdict(
        lambda: {"metric_names": set(), "instances": set(), "time_series": 0}
    )

    log.info("Обработка team/service_id...")

    for team in sorted(teams):
        if not team:
            continue

        log.info(f"[TEAM] {team}")

        q_svc = f'count by (service_id) ({{team="{team}", service_id!=""}})'
        svc_rows = safe_query(prom, q_svc)
        service_ids = set()
        if svc_rows:
            for r in svc_rows:
                sid = r["metric"].get("service_id")
                if sid:
                    service_ids.add(sid)

        q_total = f'count({{team="{team}"}})'
        total_res = safe_query(prom, q_total)
        total = int(total_res[0]["value"][1]) if total_res else 0

        q_with_svc = f'count({{team="{team}", service_id!=""}})'
        with_svc_res = safe_query(prom, q_with_svc)
        with_svc = int(with_svc_res[0]["value"][1]) if with_svc_res else 0

        q_no_svc = f'count({{team="{team}", service_id=""}})'
        no_svc_res = safe_query(prom, q_no_svc)
        no_svc = int(no_svc_res[0]["value"][1]) if no_svc_res else 0

        log.info(
            f"[TEAM] {team}: total={total}, with_service_id={with_svc}, no_service_id={no_svc}"
        )

        for service_id in sorted(service_ids):
            log.info(f"[TEAM-SVC] {team}/{service_id}")

            q_series = f'count({{team="{team}", service_id="{service_id}"}})'
            res_series = safe_query(prom, q_series)
            time_series_count = int(res_series[0]["value"][1]) if res_series else 0

            q_instances = (
                f'count by (instance) ({{team="{team}", service_id="{service_id}"}})'
            )
            inst_rows = safe_query(prom, q_instances)
            inst_list = (
                {r["metric"].get("instance", "<none>") for r in inst_rows}
                if inst_rows
                else set()
            )

            q_metrics = (
                f'count by (__name__) ({{team="{team}", service_id="{service_id}"}})'
            )
            mn_rows = safe_query(prom, q_metrics)
            metric_names = (
                {r["metric"].get("__name__", "<noname>") for r in mn_rows}
                if mn_rows
                else set()
            )

            key = f"{team}-{service_id}"
            g = groups[key]
            g["metric_names"].update(metric_names)
            g["instances"].update(inst_list)
            g["time_series"] += time_series_count

        if no_svc > 0:
            q_instances_ns = f'count by (instance) ({{team="{team}", service_id=""}})'
            inst_ns_rows = safe_query(prom, q_instances_ns)
            inst_ns = (
                {r["metric"].get("instance", "<none>") for r in inst_ns_rows}
                if inst_ns_rows
                else set()
            )

            q_metrics_ns = f'count by (__name__) ({{team="{team}", service_id=""}})'
            mn_ns_rows = safe_query(prom, q_metrics_ns)
            metric_ns = (
                {r["metric"].get("__name__", "<noname>") for r in mn_ns_rows}
                if mn_ns_rows
                else set()
            )

            key = f"{team}-<none>"
            g = groups[key]
            g["metric_names"].update(metric_ns)
            g["instances"].update(inst_ns)
            g["time_series"] += no_svc

    log.info("Обработка service_id без team...")

    q_svc_no_team = 'count by (service_id) ({team="", service_id!=""})'
    svc_no_team_rows = safe_query(prom, q_svc_no_team)
    svc_no_team_ids = (
        {r["metric"].get("service_id") for r in svc_no_team_rows}
        if svc_no_team_rows
        else set()
    )

    for service_id in sorted(svc_no_team_ids):
        if not service_id:
            continue

        log.info(f"[<none>-SVC] {service_id}")

        q_series = f'count({{team="", service_id="{service_id}"}})'
        res_series = safe_query(prom, q_series)
        time_series_count = int(res_series[0]["value"][1]) if res_series else 0

        q_instances = f'count by (instance) ({{team="", service_id="{service_id}"}})'
        inst_rows = safe_query(prom, q_instances)
        inst_list = (
            {r["metric"].get("instance", "<none>") for r in inst_rows}
            if inst_rows
            else set()
        )

        q_metrics = f'count by (__name__) ({{team="", service_id="{service_id}"}})'
        mn_rows = safe_query(prom, q_metrics)
        metric_names = (
            {r["metric"].get("__name__", "<noname>") for r in mn_rows}
            if mn_rows
            else set()
        )

        key = f"<none>-{service_id}"
        g = groups[key]
        g["metric_names"].update(metric_names)
        g["instances"].update(inst_list)
        g["time_series"] += time_series_count

    log.info("Обработка полностью неразмеченных...")

    q_unlabeled = 'count({team="", service_id=""})'
    unlabeled_res = safe_query(prom, q_unlabeled)
    unlabeled_series = int(unlabeled_res[0]["value"][1]) if unlabeled_res else 0

    if unlabeled_series > 0:
        q_inst_unl = 'count by (instance) ({team="", service_id=""})'
        inst_unl_rows = safe_query(prom, q_inst_unl)
        inst_unl = (
            {r["metric"].get("instance", "<none>") for r in inst_unl_rows}
            if inst_unl_rows
            else set()
        )

        q_mn_unl = 'count by (__name__) ({team="", service_id=""})'
        mn_unl_rows = safe_query(prom, q_mn_unl)
        mn_unl = (
            {r["metric"].get("__name__", "<noname>") for r in mn_unl_rows}
            if mn_unl_rows
            else set()
        )

        g = groups["unlabeled"]
        g["metric_names"].update(mn_unl)
        g["instances"].update(inst_unl)
        g["time_series"] += unlabeled_series

    log.info("Запись листа team_service_metrics...")

    sheet_groups = workbook.add_worksheet("team_service_metrics")
    headers_groups = [
        "team_service",
        "metric_names",
        "time_series",
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
        q = f'count by (__name__) ({{job="{job}"}})'
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
