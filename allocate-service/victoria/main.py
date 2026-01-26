import os
import sys
import logging
from collections import defaultdict

import requests
from dotenv import load_dotenv
from prometheus_api_client import PrometheusConnect

from openpyxl import Workbook
from openpyxl.styles import Font

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT_FILE = "victoriametrics_repot.xlsx"
HTTP_TIMEOUT_SEC = 60


def safe_query(prom, q):
    log.info(f"QUERY: {q}")
    try:
        url = prom.url.rstrip("/") + "/api/v1/query"
        r = requests.get(
            url,
            params={"query": q},
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


def get_teams(prom):
    try:
        teams = prom.get_label_values("team")
        teams = [t for t in teams if t is not None and str(t).strip() != ""]
        teams = sorted(set(teams))
        log.info(f"Найдено team: {len(teams)}")
        return teams
    except Exception as e:
        log.error(f"Ошибка получения team: {e}")
        return []


def get_service_ids_for_team(prom, team):
    q = f'count by (service_id) ({{team="{team}", service_id!=""}})'
    rows = safe_query(prom, q)
    if not rows:
        return []
    sids = set()
    for r in rows:
        sid = r.get("metric", {}).get("service_id")
        if sid:
            sids.add(sid)
    return sorted(sids)


def get_group_metrics(prom, team, service_id):
    match = f'team="{team}", service_id="{service_id}"'

    q_series = f'count({{{match}}})'
    res_series = safe_query(prom, q_series)
    time_series_count = int(res_series[0]["value"][1]) if res_series else 0

    q_instances = f'count by (instance) ({{{match}}})'
    inst_rows = safe_query(prom, q_instances)
    instances = (
        {r.get("metric", {}).get("instance", "<none>") for r in inst_rows}
        if inst_rows
        else set()
    )

    q_metrics = f'count by (__name__) ({{{match}}})'
    mn_rows = safe_query(prom, q_metrics)
    metric_names = (
        {r.get("metric", {}).get("__name__", "<noname>") for r in mn_rows}
        if mn_rows
        else set()
    )

    return time_series_count, instances, metric_names


def write_report(groups, out_file):
    wb = Workbook()
    ws = wb.active
    ws.title = "team_service_metrics"

    headers = ["team_service", "metric_names", "time_series", "instances_count", "instances_list"]
    bold = Font(bold=True)

    ws.append(headers)
    for cell in ws[1]:
        cell.font = bold

    for group_name in sorted(groups.keys()):
        data = groups[group_name]
        ws.append(
            [
                group_name,
                len(data["metric_names"]),
                data["time_series"],
                len(data["instances"]),
                ", ".join(sorted(data["instances"])),
            ]
        )

    wb.save(out_file)


def main():
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

    teams = get_teams(prom)

    groups = defaultdict(lambda: {"metric_names": set(), "instances": set(), "time_series": 0})

    log.info("Обработка team/service_id...")
    for team in teams:
        log.info(f"[TEAM] {team}")
        service_ids = get_service_ids_for_team(prom, team)
        if not service_ids:
            log.info(f"[TEAM] {team}: service_id не найден(ы), пропуск")
            continue

        for service_id in service_ids:
            log.info(f"[TEAM-SVC] {team}/{service_id}")

            ts, instances, metric_names = get_group_metrics(prom, team, service_id)

            key = f"{team}-{service_id}"
            g = groups[key]
            g["time_series"] += ts
            g["instances"].update(instances)
            g["metric_names"].update(metric_names)

    log.info(f"Сохранение файла {OUTPUT_FILE}...")
    write_report(groups, OUTPUT_FILE)
    log.info(f"✔ Готово. Файл {OUTPUT_FILE} создан.")


if __name__ == "__main__":
    main()
