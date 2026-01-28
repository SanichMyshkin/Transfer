import os
import sys
import time
import logging
from typing import List, Tuple, Dict

import requests
import urllib3
from dotenv import load_dotenv
from prometheus_api_client import PrometheusConnect

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT_FILE = "victoriametrics_repot.xlsx"
HTTP_TIMEOUT_SEC = 60

SLEEP_BETWEEN_QUERIES_SEC = 0.2
SLEEP_BETWEEN_GROUPS_SEC = 0.1

BAN_TEAMS: List[str] = [
    "UNAITP",
]

# Окна для оценки интервала (твоя логика)
WINDOWS: List[Tuple[str, int]] = [
    ("2m", 120),
    ("30m", 1800),
    ("1h", 3600),
    ("6h", 21600),
    ("12h", 43200),
    ("24h", 86400),
]

# Lookback для оценки длины рядов (обрезается этим окном, если retention больше)
SERIES_LENGTH_LOOKBACK = "365d"


def safe_query(prom: PrometheusConnect, q: str):
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
    finally:
        time.sleep(SLEEP_BETWEEN_QUERIES_SEC)


def label(metric: dict, key: str) -> str:
    v = metric.get(key)
    return "" if v is None or str(v).strip() == "" else str(v).strip()


def esc_label_value(s: str) -> str:
    return str(s).replace("\\", "\\\\").replace('"', '\\"')


def get_scalar_value(rows) -> float:
    if not rows:
        return 0.0
    try:
        return float(rows[0].get("value", [None, "0"])[1])
    except Exception:
        return 0.0


def discover_groups(prom: PrometheusConnect):
    groups = set()
    queries = [
        'count by (team, service_id) ({team=~".+", service_id=~".+"})',
        'count by (team, service_id) ({team!~".+", service_id=~".+"})',
        'count by (team, service_id) ({team=~".+", service_id!~".+"})',
        'count by (team, service_id) ({team!~".+", service_id!~".+"})',
    ]
    for i, q in enumerate(queries, start=1):
        log.info(f"DISCOVER[{i}/4]")
        rows = safe_query(prom, q) or []
        for r in rows:
            m = r.get("metric", {}) or {}
            groups.add((label(m, "team"), label(m, "service_id")))
    return sorted(groups)


def build_group_matchers(team: str, service_id: str) -> str:
    return ", ".join(
        [
            'team!~".+"' if team == "" else f'team="{esc_label_value(team)}"',
            'service_id!~".+"' if service_id == "" else f'service_id="{esc_label_value(service_id)}"',
        ]
    )


def metric_names_in_group(prom: PrometheusConnect, group_matchers: str) -> List[str]:
    q = f'count by (__name__) ({{{group_matchers}}})'
    rows = safe_query(prom, q) or []
    names = []
    for r in rows:
        m = r.get("metric", {}) or {}
        n = m.get("__name__")
        if n:
            names.append(n)
    return sorted(set(names))


def series_count_for_metric(prom: PrometheusConnect, group_matchers: str, metric_name: str) -> int:
    mn = esc_label_value(metric_name)
    q = f'count({{{group_matchers}, __name__="{mn}"}})'
    rows = safe_query(prom, q) or []
    return int(get_scalar_value(rows))


def pick_interval_sec_for_metric(prom: PrometheusConnect, group_matchers: str, metric_name: str) -> int:
    mn = esc_label_value(metric_name)

    for w, wsec in WINDOWS:
        q = f'count_over_time({{{group_matchers}, __name__="{mn}"}}[{w}])'
        rows = safe_query(prom, q) or []
        if not rows:
            continue

        try:
            v = float(rows[0].get("value", [None, "0"])[1])
        except Exception:
            v = 0.0

        if v >= 2.0:
            interval = int(round(wsec / (v - 1.0)))
            return max(1, interval)

        if w == "24h" and v >= 1.0:
            return 86400

    return 86400


def series_length_stats_for_metric(
    prom: PrometheusConnect,
    group_matchers: str,
    metric_name: str,
    lookback: str,
) -> Dict[str, float]:
    mn = esc_label_value(metric_name)
    sel = f'{{{group_matchers}, __name__="{mn}"}}'
    core = f'(max_over_time(timestamp({sel})[{lookback}]) - min_over_time(timestamp({sel})[{lookback}]))'

    q_avg = f'avg({core})'
    q_min = f'min({core})'
    q_max = f'max({core})'

    avg_rows = safe_query(prom, q_avg) or []
    min_rows = safe_query(prom, q_min) or []
    max_rows = safe_query(prom, q_max) or []

    return {
        "avg_len_sec": get_scalar_value(avg_rows),
        "min_len_sec": get_scalar_value(min_rows),
        "max_len_sec": get_scalar_value(max_rows),
    }


def autosize_columns(ws):
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            v = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, len(v))
        ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 70)


def write_report(rows, out_file: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "report"

    headers = [
        "team",
        "service_id",
        "__name__",
        "series_count",
        "interval_sec_est",
        "points_per_day_est",
        f"avg_series_len_sec@{SERIES_LENGTH_LOOKBACK}",
        f"min_series_len_sec@{SERIES_LENGTH_LOOKBACK}",
        f"max_series_len_sec@{SERIES_LENGTH_LOOKBACK}",
    ]
    ws.append(headers)
    bold = Font(bold=True)
    for c in ws[1]:
        c.font = bold
    ws.freeze_panes = "A2"

    for r in rows:
        ws.append(r)

    autosize_columns(ws)
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

    log.info("Поиск групп (team/service_id)...")
    groups = discover_groups(prom)
    log.info(f"Всего уникальных групп: {len(groups)}")

    out_rows = []

    for team, service_id in groups:
        if team == "" and service_id == "":
            log.info("[GROUP] empty team & service_id -> skip")
            continue

        if team in BAN_TEAMS:
            log.info(f"[GROUP] team={team} is banned -> skip")
            continue

        group_tag = f"team={team or '<empty>'} service_id={service_id or '<empty>'}"
        log.info(f"[GROUP] {group_tag}")

        group_matchers = build_group_matchers(team, service_id)

        metrics = metric_names_in_group(prom, group_matchers)
        if not metrics:
            log.info("  no metrics -> skip group")
            time.sleep(SLEEP_BETWEEN_GROUPS_SEC)
            continue

        log.info(f"  metrics={len(metrics)}")

        for mn in metrics:
            scount = series_count_for_metric(prom, group_matchers, mn)
            if scount <= 0:
                continue

            interval_sec = pick_interval_sec_for_metric(prom, group_matchers, mn)
            points_day = scount * (86400.0 / float(interval_sec))

            lens = series_length_stats_for_metric(
                prom=prom,
                group_matchers=group_matchers,
                metric_name=mn,
                lookback=SERIES_LENGTH_LOOKBACK,
            )

            out_rows.append(
                [
                    team or "<empty_team>",
                    service_id or "<empty_service_id>",
                    mn,
                    int(scount),
                    int(interval_sec),
                    int(points_day),
                    int(lens["avg_len_sec"]),
                    int(lens["min_len_sec"]),
                    int(lens["max_len_sec"]),
                ]
            )

        time.sleep(SLEEP_BETWEEN_GROUPS_SEC)

    if not out_rows:
        log.warning("Нет данных для отчёта.")
        sys.exit(0)

    log.info(f"Сохранение файла {OUTPUT_FILE}...")
    write_report(out_rows, OUTPUT_FILE)
    log.info(f"✔ Готово. Файл {OUTPUT_FILE} создан.")


if __name__ == "__main__":
    main()
