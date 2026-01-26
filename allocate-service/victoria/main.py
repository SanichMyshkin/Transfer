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

MISSING = "<missing>"


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


def _label(metric: dict, key: str) -> str:
    v = metric.get(key)
    if v is None or str(v).strip() == "":
        return MISSING
    return str(v).strip()


def discover_groups(prom):
    """
    Собираем все комбинации (team, service_id), включая отсутствующие лейблы.
    Отдельно считаем 'unlabled' (оба лейбла отсутствуют).
    """
    groups = set()

    queries = [
        # есть и team и service_id
        ('count by (team, service_id) ({team=~".+", service_id=~".+"})', "both_present"),
        # team отсутствует, service_id есть
        ('count by (team, service_id) ({team!~".+", service_id=~".+"})', "missing_team"),
        # team есть, service_id отсутствует
        ('count by (team, service_id) ({team=~".+", service_id!~".+"})', "missing_service"),
        # оба отсутствуют (unlabled)
        ('count by (team, service_id) ({team!~".+", service_id!~".+"})', "both_missing"),
    ]

    for q, tag in queries:
        rows = safe_query(prom, q) or []
        log.info(f"Найдено групп ({tag}): {len(rows)}")
        for r in rows:
            metric = r.get("metric", {}) or {}
            team = _label(metric, "team")
            service_id = _label(metric, "service_id")
            groups.add((team, service_id))

    # Группы без обоих лейблов будут как (MISSING, MISSING)
    return sorted(groups)


def build_matchers(team: str, service_id: str) -> str:
    parts = []
    if team == MISSING:
        parts.append('team!~".+"')  # отсутствует или пустой
    else:
        parts.append(f'team="{team}"')

    if service_id == MISSING:
        parts.append('service_id!~".+"')  # отсутствует или пустой
    else:
        parts.append(f'service_id="{service_id}"')

    return ", ".join(parts)


def get_group_metrics(prom, team: str, service_id: str):
    matchers = build_matchers(team, service_id)

    q_series = f'count({{{matchers}}})'
    res_series = safe_query(prom, q_series)
    time_series_count = int(res_series[0]["value"][1]) if res_series else 0

    q_instances = f'count by (instance) ({{{matchers}}})'
    inst_rows = safe_query(prom, q_instances)
    instances = (
        {r.get("metric", {}).get("instance", "<none>") for r in inst_rows}
        if inst_rows
        else set()
    )

    q_metrics = f'count by (__name__) ({{{matchers}}})'
    mn_rows = safe_query(prom, q_metrics)
    metric_names = (
        {r.get("metric", {}).get("__name__", "<noname>") for r in mn_rows}
        if mn_rows
        else set()
    )

    return time_series_count, instances, metric_names


def write_report(groups_map, unlabled_row, out_file):
    wb = Workbook()

    bold = Font(bold=True)

    ws = wb.active
    ws.title = "team_service_metrics"
    headers = ["team", "service_id", "metric_names", "time_series", "instances_count", "instances_list"]
    ws.append(headers)
    for cell in ws[1]:
        cell.font = bold

    for (team, service_id) in sorted(groups_map.keys()):
        data = groups_map[(team, service_id)]
        ws.append(
            [
                team,
                service_id,
                len(data["metric_names"]),
                data["time_series"],
                len(data["instances"]),
                ", ".join(sorted(data["instances"])),
            ]
        )

    ws2 = wb.create_sheet("unlabled")
    headers2 = ["bucket", "metric_names", "time_series", "instances_count", "instances_list"]
    ws2.append(headers2)
    for cell in ws2[1]:
        cell.font = bold

    if unlabled_row is None:
        ws2.append(["unlabled", 0, 0, 0, ""])
    else:
        ws2.append(
            [
                "unlabled",
                len(unlabled_row["metric_names"]),
                unlabled_row["time_series"],
                len(unlabled_row["instances"]),
                ", ".join(sorted(unlabled_row["instances"])),
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

    groups_map = defaultdict(lambda: {"metric_names": set(), "instances": set(), "time_series": 0})
    unlabled_row = None

    log.info("Сбор метрик по группам...")
    for team, service_id in discovered:
        if team == MISSING and service_id == MISSING:
            log.info("[UNLABLED] (нет team и service_id)")
        else:
            log.info(f"[GROUP] team={team} service_id={service_id}")

        ts, instances, metric_names = get_group_metrics(prom, team, service_id)

        if team == MISSING and service_id == MISSING:
            unlabled_row = {"metric_names": metric_names, "instances": instances, "time_series": ts}
            continue

        g = groups_map[(team, service_id)]
        g["time_series"] += ts
        g["instances"].update(instances)
        g["metric_names"].update(metric_names)

    log.info(f"Сохранение файла {OUTPUT_FILE}...")
    write_report(groups_map, unlabled_row, OUTPUT_FILE)
    log.info(f"✔ Готово. Файл {OUTPUT_FILE} создан.")


if __name__ == "__main__":
    main()
