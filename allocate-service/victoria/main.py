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

METRICS_LIMIT = 2


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


def safe_get_json(url: str, params: dict | None = None):
    try:
        r = requests.get(url, params=params or {}, verify=False, timeout=HTTP_TIMEOUT_SEC)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"Ошибка выполнения GET {url}: {e}")
        return None


def label(metric: dict, key: str) -> str:
    v = metric.get(key)
    return "" if v is None or str(v).strip() == "" else str(v).strip()


def get_metric_names(vm_url: str, limit: int):
    url = vm_url.rstrip("/") + "/api/v1/label/__name__/values"
    data = safe_get_json(url)
    if not data or data.get("status") != "success":
        log.error(f"Bad response for `{url}`: {data}")
        return []
    names = data.get("data", []) or []
    names = [n for n in names if n and str(n).strip() != ""]
    names = sorted(set(names))
    return names[: max(0, int(limit))]


def discover_groups(prom):
    groups = set()
    queries = [
        'count by (team, service_id) ({team=~".+", service_id=~".+"})',
        'count by (team, service_id) ({team!~".+", service_id=~".+"})',
        'count by (team, service_id) ({team=~".+", service_id!~".+"})',
        'count by (team, service_id) ({team!~".+", service_id!~".+"})',
    ]

    for q in queries:
        rows = safe_query(prom, q) or []
        for r in rows:
            m = r.get("metric", {}) or {}
            groups.add((label(m, "team"), label(m, "service_id")))

    return sorted(groups)


def build_matchers(team: str, service_id: str) -> str:
    return ", ".join(
        [
            'team!~".+"' if team == "" else f'team="{team}"',
            'service_id!~".+"' if service_id == "" else f'service_id="{service_id}"',
        ]
    )


def get_group_metrics(prom, team: str, service_id: str):
    m = build_matchers(team, service_id)

    r = safe_query(prom, f'count({{{m}}})')
    ts = int(r[0]["value"][1]) if r else 0

    ir = safe_query(prom, f'count by (instance) ({{{m}}})') or []
    instances = {x.get("metric", {}).get("instance", "<none>") for x in ir}

    mr = safe_query(prom, f'count by (__name__) ({{{m}}})') or []
    names = {x.get("metric", {}).get("__name__", "<noname>") for x in mr}

    return ts, instances, names


def discover_groups_for_metric(prom, metric_name: str):
    groups = set()
    queries = [
        f'count by (team, service_id) ({metric_name}{{team=~".+", service_id=~".+"}})',
        f'count by (team, service_id) ({metric_name}{{team!~".+", service_id=~".+"}})',
        f'count by (team, service_id) ({metric_name}{{team=~".+", service_id!~".+"}})',
        f'count by (team, service_id) ({metric_name}{{team!~".+", service_id!~".+"}})',
    ]

    for q in queries:
        rows = safe_query(prom, q) or []
        for r in rows:
            m = r.get("metric", {}) or {}
            groups.add((label(m, "team"), label(m, "service_id")))

    return sorted(groups)


def get_group_metric_usage(prom, metric_name: str, team: str, service_id: str):
    m = build_matchers(team, service_id)
    sel = f'{metric_name}{{{m}}}'

    r_series = safe_query(prom, f'count({sel})')
    series_cnt = int(r_series[0]["value"][1]) if r_series else 0

    r_points = safe_query(prom, f'sum(count_over_time({sel}[24h]))')
    points_24h = int(float(r_points[0]["value"][1])) if r_points else 0

    return series_cnt, points_24h


def write_report(groups, metric_usage, out_file):
    wb = Workbook()
    ws = wb.active
    ws.title = "team_service_metrics"

    headers = [
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
                "unlabled" if team == "" and service_id == "" else team,
                service_id,
                len(d["metric_names"]),
                d["time_series"],
                len(d["instances"]),
                ", ".join(sorted(d["instances"])),
            ]
        )

    ws2 = wb.create_sheet("metrics_24h_usage")
    headers2 = ["metric", "team", "service_id", "time_series", "datapoints_24h"]
    ws2.append(headers2)
    for c in ws2[1]:
        c.font = bold

    for (metric, team, service_id), d in sorted(metric_usage.items()):
        ws2.append(
            [
                metric,
                "unlabled" if team == "" and service_id == "" else team,
                service_id,
                d["series"],
                d["points_24h"],
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

    log.info(f"Получение списка метрик (__name__) и ограничение METRICS_LIMIT={METRICS_LIMIT}...")
    metric_names = get_metric_names(vm_url, METRICS_LIMIT)
    log.info(f"Метрик к обработке: {len(metric_names)}")

    metric_usage = defaultdict(lambda: {"series": 0, "points_24h": 0})

    log.info("Сбор использования по метрикам (time_series и datapoints за 24h)...")
    for metric in metric_names:
        log.info(f"[METRIC] {metric}")
        mgroups = discover_groups_for_metric(prom, metric)
        for team, service_id in mgroups:
            log.info(
                "[UNLABLED] (нет team и service_id)"
                if team == "" and service_id == ""
                else f"[GROUP] team={team} service_id={service_id}"
            )
            series_cnt, points_24h = get_group_metric_usage(prom, metric, team, service_id)
            metric_usage[(metric, team, service_id)]["series"] += series_cnt
            metric_usage[(metric, team, service_id)]["points_24h"] += points_24h

    log.info(f"Сохранение файла {OUTPUT_FILE}...")
    write_report(groups, metric_usage, OUTPUT_FILE)
    log.info(f"✔ Готово. Файл {OUTPUT_FILE} создан.")


if __name__ == "__main__":
    main()
