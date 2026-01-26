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


def label(metric: dict, key: str) -> str:
    v = metric.get(key)
    return "" if v is None or str(v).strip() == "" else str(v).strip()


def discover_groups(prom):
    groups = set()
    queries = [
        ('count by (team, service_id) ({team=~".+", service_id=~".+"})', "both_present"),
        ('count by (team, service_id) ({team!~".+", service_id=~".+"})', "missing_team"),
        ('count by (team, service_id) ({team=~".+", service_id!~".+"})', "missing_service"),
        ('count by (team, service_id) ({team!~".+", service_id!~".+"})', "both_missing"),
    ]

    for q, tag in queries:
        rows = safe_query(prom, q) or []
        log.info(f"Найдено групп ({tag}): {len(rows)}")
        for r in rows:
            m = r.get("metric", {}) or {}
            groups.add((label(m, "team"), label(m, "service_id")))

    return sorted(groups)


def build_matchers(team: str, service_id: str) -> str:
    parts = []
    parts.append('team!~".+"' if team == "" else f'team="{team}"')
    parts.append('service_id!~".+"' if service_id == "" else f'service_id="{service_id}"')
    return ", ".join(parts)


def get_group_metrics(prom, team: str, service_id: str):
    matchers = build_matchers(team, service_id)

    r = safe_query(prom, f'count({{{matchers}}})')
    ts = int(r[0]["value"][1]) if r else 0

    ir = safe_query(prom, f'count by (instance) ({{{matchers}}})') or []
    instances = {x.get("metric", {}).get("instance", "<none>") for x in ir}

    mr = safe_query(prom, f'count by (__name__) ({{{matchers}}})') or []
    names = {x.get("metric", {}).get("__name__", "<noname>") for x in mr}

    return ts, instances, names


def write_report(groups, out_file):
    wb = Workbook()
    ws = wb.active
    ws.title = "team_service_metrics"

    headers = [
        "bucket",
        "team",
        "service_id",
        "metric_names",
        "time_series",
        "instances_count",
        "instances_list",
    ]
    bold = Font(bold=True)

    ws.append(headers)
    for c in ws[1]:
        c.font = bold

    for (team, service_id), d in sorted(groups.items()):
        ws.append(
            [
                "unlabled" if team == "" and service_id == "" else "",
                team,
                service_id,
                len(d["metric_names"]),
                d["time_series"],
                len(d["instances"]),
                ", ".join(sorted(d["instances"])),
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

    log.info("Поиск групп (team/service_id), включая отсутствующие лейблы...")
    discovered = discover_groups(prom)
    log.info(f"Всего уникальных групп: {len(discovered)}")

    groups = defaultdict(lambda: {"metric_names": set(), "instances": set(), "time_series": 0})

    log.info("Сбор метрик по группам...")
    for team, service_id in discovered:
        log.info(
            "[UNLABLED] (нет team и service_id)"
            if team == "" and service_id == ""
            else f"[GROUP] team={team} service_id={service_id}"
        )

        ts, inst, names = get_group_metrics(prom, team, service_id)
        g = groups[(team, service_id)]
        g["time_series"] += ts
        g["instances"].update(inst)
        g["metric_names"].update(names)

    log.info(f"Сохранение файла {OUTPUT_FILE}...")
    write_report(groups, OUTPUT_FILE)
    log.info(f"✔ Готово. Файл {OUTPUT_FILE} создан.")


if __name__ == "__main__":
    main()
