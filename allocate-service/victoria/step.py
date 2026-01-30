import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import requests
import urllib3
from dotenv import load_dotenv

from openpyxl import Workbook
from openpyxl.styles import Font

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT_FILE = os.getenv("OUTPUT_FILE", "victoriametrics_group_samples_24h.xlsx")
HTTP_TIMEOUT_SEC = 30

SLEEP_BETWEEN_REQUESTS_SEC = float(os.getenv("SLEEP_BETWEEN_REQUESTS_SEC", "0.2"))
TOP_GROUPS = int(os.getenv("TOP_GROUPS", "0"))
MIN_TIME_SERIES = int(os.getenv("MIN_TIME_SERIES", "0"))
INCLUDE_METRIC_NAMES = os.getenv("INCLUDE_METRIC_NAMES", "false").strip().lower() in {"1", "true", "yes", "y", "on"}


def http_query(vm_url: str, q: str):
    url = vm_url.rstrip("/") + "/api/v1/query"
    log.info(f"QUERY: {q}")
    try:
        r = requests.get(url, params={"query": q}, verify=False, timeout=HTTP_TIMEOUT_SEC)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "success":
            log.error(f"Bad response for `{q}`: {data}")
            return None
        return data.get("data", {}).get("result", [])
    except Exception as e:
        log.error(f"Ошибка выполнения `{q}`: {e}")
        return None


def http_query_range(vm_url: str, q: str, start_ts: float, end_ts: float, step_sec: int):
    url = vm_url.rstrip("/") + "/api/v1/query_range"
    log.info(f"QUERY_RANGE: {q}")
    try:
        r = requests.get(
            url,
            params={
                "query": q,
                "start": str(start_ts),
                "end": str(end_ts),
                "step": str(step_sec),
            },
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
        log.error(f"Ошибка выполнения query_range `{q}`: {e}")
        return None


def label(metric: dict, key: str) -> str:
    v = metric.get(key)
    return "" if v is None or str(v).strip() == "" else str(v).strip()


def discover_groups(vm_url: str):
    groups = set()
    queries = [
        'count by (team, service_id) ({team=~".+", service_id=~".+"})',
        'count by (team, service_id) ({team!~".+", service_id=~".+"})',
        'count by (team, service_id) ({team=~".+", service_id!~".+"})',
        'count by (team, service_id) ({team!~".+", service_id!~".+"})',
    ]

    for q in queries:
        rows = http_query(vm_url, q) or []
        for r in rows:
            m = r.get("metric", {}) or {}
            groups.add((label(m, "team"), label(m, "service_id")))

        if SLEEP_BETWEEN_REQUESTS_SEC > 0:
            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

    return sorted(groups)


def build_matchers(team: str, service_id: str) -> str:
    return ", ".join(
        [
            'team!~".+"' if team == "" else f'team="{team}"',
            'service_id!~".+"' if service_id == "" else f'service_id="{service_id}"',
        ]
    )


def get_group_time_series(vm_url: str, team: str, service_id: str) -> int:
    m = build_matchers(team, service_id)
    r = http_query(vm_url, f"count({{{m}}})")
    if not r:
        return 0
    try:
        return int(float(r[0]["value"][1]))
    except Exception:
        return 0


def get_group_metric_names_count(vm_url: str, team: str, service_id: str) -> int:
    m = build_matchers(team, service_id)
    mr = http_query(vm_url, f"count by (__name__) ({{{m}}})") or []
    names = set()
    for x in mr:
        n = (x.get("metric", {}) or {}).get("__name__")
        if n is None:
            continue
        s = str(n).strip()
        if s:
            names.add(s)
    return len(names)


def get_group_samples_24h(vm_url: str, team: str, service_id: str, end_dt: datetime) -> tuple[int, int]:
    m = build_matchers(team, service_id)
    q = f"sum(count_over_time({{{m}}}[1h]))"

    start_dt = end_dt - timedelta(hours=24)
    start_ts = start_dt.timestamp()
    end_ts = end_dt.timestamp()

    res = http_query_range(vm_url, q, start_ts, end_ts, 3600) or []
    total = 0.0
    points = 0

    for row in res:
        vals = row.get("values") or []
        for tv in vals:
            if not isinstance(tv, list) or len(tv) < 2:
                continue
            try:
                total += float(tv[1])
                points += 1
            except Exception:
                continue

    return int(round(total)), points


def write_report(rows: list[dict], out_file: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "team_service_samples_24h"

    headers = ["team", "service_id", "time_series", "samples_24h", "hours_points"]
    if INCLUDE_METRIC_NAMES:
        headers.insert(2, "metric_names_count")

    bold = Font(bold=True)
    ws.append(headers)
    for c in ws[1]:
        c.font = bold

    for r in rows:
        team = r["team"]
        service_id = r["service_id"]
        out_team = "unlabeled" if team == "" and service_id == "" else team

        line = [out_team, service_id, r["time_series"], r["samples_24h"], r["hours_points"]]
        if INCLUDE_METRIC_NAMES:
            line.insert(2, r["metric_names_count"])
        ws.append(line)

    wb.save(out_file)


def main():
    load_dotenv()
    vm_url = os.getenv("VM_URL", "").strip()
    if not vm_url:
        log.error("VM_URL отсутствует")
        sys.exit(1)

    log.info(f"VM_URL: {vm_url}")
    log.info("Поиск групп (team/service_id), включая отсутствующие лейблы...")
    groups = discover_groups(vm_url)
    log.info(f"Всего уникальных групп: {len(groups)}")

    log.info("Считаю time_series по группам...")
    group_stats = []
    total_series = 0
    for i, (team, service_id) in enumerate(groups, 1):
        ts = get_group_time_series(vm_url, team, service_id)
        total_series += ts
        group_stats.append({"team": team, "service_id": service_id, "time_series": ts})

        if SLEEP_BETWEEN_REQUESTS_SEC > 0:
            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

    group_stats.sort(key=lambda x: x["time_series"], reverse=True)

    selected = []
    for g in group_stats:
        if MIN_TIME_SERIES > 0 and g["time_series"] < MIN_TIME_SERIES:
            continue
        selected.append(g)
        if TOP_GROUPS > 0 and len(selected) >= TOP_GROUPS:
            break

    log.info(f"Групп для подсчета samples_24h: {len(selected)} (TOP_GROUPS={TOP_GROUPS}, MIN_TIME_SERIES={MIN_TIME_SERIES})")

    end_dt = datetime.now(timezone.utc)

    out_rows = []
    for i, g in enumerate(selected, 1):
        team = g["team"]
        service_id = g["service_id"]

        log.info(
            f"[{i}/{len(selected)}] "
            + ("[UNLABELED]" if team == "" and service_id == "" else f"team={team} service_id={service_id}")
            + f" time_series={g['time_series']}"
        )

        metric_names_count = 0
        if INCLUDE_METRIC_NAMES:
            metric_names_count = get_group_metric_names_count(vm_url, team, service_id)
            if SLEEP_BETWEEN_REQUESTS_SEC > 0:
                time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

        samples_24h, hours_points = get_group_samples_24h(vm_url, team, service_id, end_dt=end_dt)

        row = {
            "team": team,
            "service_id": service_id,
            "time_series": g["time_series"],
            "samples_24h": samples_24h,
            "hours_points": hours_points,
            "metric_names_count": metric_names_count,
        }
        out_rows.append(row)

        if SLEEP_BETWEEN_REQUESTS_SEC > 0:
            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

    log.info(f"Сохранение файла {OUTPUT_FILE}...")
    write_report(out_rows, OUTPUT_FILE)
    log.info(f"✔ Готово. Файл {OUTPUT_FILE} создан.")


if __name__ == "__main__":
    main()
