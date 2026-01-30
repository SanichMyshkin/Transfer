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

OUTPUT_FILE = "victoriametrics_samples_by_group.xlsx"
HTTP_TIMEOUT_SEC = 30
SLEEP_SEC = 0.3


def http_query(vm_url: str, query: str):
    url = vm_url.rstrip("/") + "/api/v1/query"
    log.info(f"QUERY: {query}")
    r = requests.get(
        url,
        params={"query": query},
        verify=False,
        timeout=HTTP_TIMEOUT_SEC,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(data)
    return data["data"]["result"]


def http_query_range(vm_url: str, query: str, start_ts: float, end_ts: float, step: int):
    url = vm_url.rstrip("/") + "/api/v1/query_range"
    log.info(f"QUERY_RANGE: {query}")
    r = requests.get(
        url,
        params={
            "query": query,
            "start": start_ts,
            "end": end_ts,
            "step": step,
        },
        verify=False,
        timeout=HTTP_TIMEOUT_SEC,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(data)
    return data["data"]["result"]


def label(metric: dict, key: str) -> str:
    v = metric.get(key)
    return "" if v is None else str(v).strip()


def discover_groups(vm_url: str):
    groups = set()
    queries = [
        'count by (team, service_id) ({team=~".+", service_id=~".+"})',
        'count by (team, service_id) ({team!~".+", service_id=~".+"})',
        'count by (team, service_id) ({team=~".+", service_id!~".+"})',
        'count by (team, service_id) ({team!~".+", service_id!~".+"})',
    ]

    for q in queries:
        rows = http_query(vm_url, q)
        for r in rows:
            m = r.get("metric", {})
            groups.add((label(m, "team"), label(m, "service_id")))
        time.sleep(SLEEP_SEC)

    return sorted(groups)


def build_matchers(team: str, service_id: str) -> str:
    return ", ".join(
        [
            'team!~".+"' if team == "" else f'team="{team}"',
            'service_id!~".+"' if service_id == "" else f'service_id="{service_id}"',
        ]
    )


def samples_24h_by_group(vm_url: str, team: str, service_id: str, end_dt: datetime) -> int:
    matchers = build_matchers(team, service_id)
    query = f"sum(count_over_time({{{matchers}}}[1h]))"

    start_dt = end_dt - timedelta(hours=24)

    res = http_query_range(
        vm_url,
        query,
        start_dt.timestamp(),
        end_dt.timestamp(),
        3600,
    )

    total = 0
    for row in res:
        for ts, value in row.get("values", []):
            total += float(value)

    return int(total)


def write_report(rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "samples_24h"

    headers = ["team", "service_id", "samples_24h"]
    ws.append(headers)
    for c in ws[1]:
        c.font = Font(bold=True)

    for r in rows:
        ws.append([r["team"], r["service_id"], r["samples_24h"]])

    wb.save(OUTPUT_FILE)


def main():
    load_dotenv()
    vm_url = os.getenv("VM_URL")
    if not vm_url:
        log.error("VM_URL не задан")
        sys.exit(1)

    log.info("Собираю группы (team / service_id)...")
    groups = discover_groups(vm_url)
    log.info(f"Групп найдено: {len(groups)}")

    end_dt = datetime.now(timezone.utc)

    rows = []
    for team, service_id in groups:
        log.info(
            "[UNLABELED]"
            if team == "" and service_id == ""
            else f"team={team} service_id={service_id}"
        )

        samples = samples_24h_by_group(vm_url, team, service_id, end_dt)
        rows.append(
            {
                "team": team,
                "service_id": service_id,
                "samples_24h": samples,
            }
        )
        time.sleep(SLEEP_SEC)

    rows.sort(key=lambda x: (x["team"], x["service_id"]))

    log.info(f"Сохраняю отчет: {OUTPUT_FILE}")
    write_report(rows)
    log.info("✔ Готово")


if __name__ == "__main__":
    main()
