import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
import re
from collections import defaultdict

import requests
import urllib3
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

load_dotenv()

OUTPUT_FILE = os.getenv("OUT_FILE", "victoria_report.xlsx")
HTTP_TIMEOUT_SEC = 30
SLEEP_SEC = 0.1

BAN_TEAMS = ["UNAITP"]
BAN_SERVICE_IDS = [15473]
BAN_BUSINESS_TYPES = []

TEAM_SERVICE_ID_OVERRIDES = {

}

SKIP_EMPTY_BUSINESS_TYPE = True
EXCLUDE_NO_SERVICE_ID_AT_QUERY = False
EXTRAPOLATION_DAYS = 90

SD_FILE = os.getenv("SD_FILE")
BK_FILE = os.getenv("BK_FILE")

TEAM_TAIL_ID_RE = re.compile(r"^(.*)-(\d+)$")

WINDOW_HOURS = 72
WINDOW_DAYS = WINDOW_HOURS / 24.0

REPORT_COLS = [
    "Тип бизнеса",
    "Наименование сервиса",
    "КОД",
    "Владелец сервиса",
    "samples_72h",
    "эксрополяция",
    "% от общего числа",
]

UNACC_COLS = ["stage", "team", "service_id", "metric", "reason", "detail"]


def is_all_zeros(s: str) -> bool:
    s = (s or "").strip()
    return bool(s) and set(s) == {"0"}


def normalize_sid(sid: str) -> str:
    sid = (sid or "").strip()
    if not sid:
        return ""
    if is_all_zeros(sid):
        return ""
    return sid


def build_team_to_sid_map(metric_keys):
    team_sids = defaultdict(set)

    for team, sid, _ in metric_keys:
        sid = normalize_sid(sid)
        if sid:
            team_sids[team].add(sid)

    team_to_sid = {}
    ambiguous_teams = set()

    for team, sids in team_sids.items():
        if len(sids) == 1:
            team_to_sid[team] = next(iter(sids))
        elif len(sids) > 1:
            ambiguous_teams.add(team)

    return team_to_sid, ambiguous_teams


def http_query(vm_url: str, query: str):
    url = vm_url.rstrip("/") + "/api/v1/query"
    log.info(f"QUERY: {query}")
    r = requests.get(url, params={"query": query}, verify=False, timeout=HTTP_TIMEOUT_SEC)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(data)
    return data["data"]["result"]


def http_query_range(vm_url: str, query: str, start_ts: float, end_ts: float, step: int):
    url = vm_url.rstrip("/") + "/api/v1/query_range"
    r = requests.get(
        url,
        params={"query": query, "start": start_ts, "end": end_ts, "step": step},
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


def discover_metric_keys(vm_url: str, add_unacc_once):
    metric_keys = set()

    queries = [
        'count by (team, service_id, __name__) ({team=~".+", service_id=~".+"})',
        'count by (team, service_id, __name__) ({team!~".+", service_id=~".+"})',
        'count by (team, service_id, __name__) ({team=~".+", service_id!~".+"})',
        'count by (team, service_id, __name__) ({team!~".+", service_id!~".+"})',
    ]

    for q in queries:
        rows = http_query(vm_url, q)
        for r in rows or []:
            m = r.get("metric", {}) or {}
            team = label(m, "team")
            service_id = normalize_sid(label(m, "service_id"))
            metric = label(m, "__name__")

            if metric:
                metric_keys.add((team, service_id, metric))

        time.sleep(SLEEP_SEC)

    return sorted(metric_keys)


def samples_72h_for_metric(vm_url: str, metric_name: str, team: str, service_id: str, end_dt: datetime) -> int:
    matchers = []
    matchers.append(f'team="{team}"')
    if service_id:
        matchers.append(f'service_id="{service_id}"')
    else:
        matchers.append('service_id!~".+"')
    matchers.append(f'__name__="{metric_name}"')

    q = f"sum(count_over_time({{{', '.join(matchers)}}}[1h]))"
    start_dt = end_dt - timedelta(hours=WINDOW_HOURS)

    res = http_query_range(
        vm_url,
        q,
        start_dt.timestamp(),
        end_dt.timestamp(),
        3600,
    )

    total = 0.0
    for row in res or []:
        for tv in row.get("values", []):
            if not isinstance(tv, list) or len(tv) < 2:
                continue
            total += float(tv[1])
    return int(total)


def write_report(df_report: pd.DataFrame, df_unacc: pd.DataFrame):
    wb = Workbook()
    bold = Font(bold=True)

    ws = wb.active
    ws.title = "samples_72h"

    ws.append(REPORT_COLS)
    for c in ws[1]:
        c.font = bold

    for row in df_report.itertuples(index=False):
        ws.append(list(row))

    if "% от общего числа" in df_report.columns:
        col_idx = df_report.columns.get_loc("% от общего числа") + 1
        for cell in ws.iter_cols(min_col=col_idx, max_col=col_idx, min_row=2):
            for c in cell:
                c.number_format = "0.00000"

    ws2 = wb.create_sheet("Unaccounted")

    if df_unacc.empty:
        ws2.append(["No data"])
        ws2["A1"].font = bold
    else:
        ws2.append(UNACC_COLS)
        for c in ws2[1]:
            c.font = bold
        for row in df_unacc.itertuples(index=False):
            ws2.append(list(row))

    wb.save(OUTPUT_FILE)


def main():
    vm_url = os.getenv("VM_URL", "").strip()
    if not vm_url:
        log.error("VM_URL не задан")
        sys.exit(1)

    log.info("VM_URL=%s", vm_url)
    log.info("WINDOW_HOURS=%s", WINDOW_HOURS)
    log.info("EXTRAPOLATION_DAYS=%s", EXTRAPOLATION_DAYS)

    unacc_map = {}

    def add_unacc_once(stage, team, service_id, metric, reason, detail):
        key = (team or "", service_id or "", metric or "")
        if key in unacc_map:
            return
        unacc_map[key] = {
            "stage": stage,
            "team": team or "",
            "service_id": service_id or "",
            "metric": metric or "",
            "reason": reason,
            "detail": detail,
        }

    log.info("Discover metrics...")
    metric_keys = discover_metric_keys(vm_url, add_unacc_once)

    team_to_sid_map, ambiguous_teams = build_team_to_sid_map(metric_keys)

    overrides = {}
    for k, v in (TEAM_SERVICE_ID_OVERRIDES or {}).items():
        kk = (k or "").strip()
        vv = normalize_sid(v)
        if kk and vv:
            overrides[kk] = vv

    log.info("Team->SID inferred: %d", len(team_to_sid_map))
    log.info("Ambiguous teams: %d", len(ambiguous_teams))
    log.info("Overrides: %d", len(overrides))

    end_dt = datetime.now(timezone.utc)
    metric_rows = []

    for idx, (team, service_id, metric) in enumerate(metric_keys, 1):
        service_id = normalize_sid(service_id)

        ov = overrides.get(team)
        if ov:
            service_id = ov
        elif not service_id:
            if team in ambiguous_teams:
                add_unacc_once(
                    "infer",
                    team,
                    "",
                    metric,
                    "ambiguous_service_id",
                    "multiple service_id detected for team",
                )
                continue

            inferred_sid = team_to_sid_map.get(team)
            if inferred_sid:
                service_id = inferred_sid

        log.info(f"[{idx}/{len(metric_keys)}] team={team} service_id={service_id} metric={metric}")

        try:
            s = samples_72h_for_metric(vm_url, metric, team, service_id, end_dt)
            metric_rows.append(
                {
                    "Тип бизнеса": "",
                    "Наименование сервиса": team,
                    "КОД": service_id,
                    "Владелец сервиса": "",
                    "samples_72h": s,
                    "эксрополяция": int(round((s / WINDOW_DAYS) * EXTRAPOLATION_DAYS)) if WINDOW_DAYS else 0,
                }
            )
        except Exception as e:
            add_unacc_once("samples", team, service_id, metric, "samples_failed", str(e))

        time.sleep(SLEEP_SEC)

    df_report = pd.DataFrame(metric_rows)
    if not df_report.empty:
        total = float(df_report["samples_72h"].sum())
        df_report["% от общего числа"] = (df_report["samples_72h"] / total * 100.0 if total else 0.0).round(5)
        df_report = df_report.reindex(columns=REPORT_COLS)
        df_report = df_report.sort_values(["samples_72h"], ascending=False).reset_index(drop=True)
    else:
        df_report = pd.DataFrame(columns=REPORT_COLS)

    df_unacc = pd.DataFrame(list(unacc_map.values()))
    if not df_unacc.empty:
        df_unacc = df_unacc.reindex(columns=UNACC_COLS).fillna("")
        df_unacc = df_unacc.sort_values(["stage", "reason", "team", "service_id", "metric"]).reset_index(drop=True)

    log.info("Saving report: %s", OUTPUT_FILE)
    write_report(df_report, df_unacc)
    log.info("✔ Done")


if __name__ == "__main__":
    main()