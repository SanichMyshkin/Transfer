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

OUTPUT_FILE = os.getenv("OUT_FILE", "victoriareport.xlsx")
HTTP_TIMEOUT_SEC = 30
SLEEP_SEC = 0.1

BAN_TEAMS = ["UNAITP"]
BAN_SERVICE_IDS = [15473]
BAN_BUSINESS_TYPES = []

SKIP_EMPTY_BUSINESS_TYPE = True
EXCLUDE_NO_SERVICE_ID_AT_QUERY = False
EXTRAPOLATION_DAYS = 90

SD_FILE = os.getenv("SD_FILE")
BK_FILE = os.getenv("BK_FILE")

TEAM_TAIL_ID_RE = re.compile(r"^(.*)-(\d+)$")

WINDOW_HOURS = 48
WINDOW_DAYS = WINDOW_HOURS / 24.0

REPORT_COLS = [
    "Тип бизнеса",
    "Наименование сервиса",
    "КОД",
    "Владелец сервиса",
    "samples_48h",
    "эксрополяция",
    "% от общего числа",
]

UNACC_COLS = ["stage", "team", "service_id", "metric", "reason", "detail"]


def build_ban_set(ban_list):
    if not isinstance(ban_list, (list, tuple, set)):
        raise SystemExit("BAN_SERVICE_IDS должен быть list / tuple / set")
    return {str(x).strip() for x in ban_list if str(x).strip()}


ban_service_set = build_ban_set(BAN_SERVICE_IDS)
ban_business_set = {
    " ".join(str(x).replace(",", " ").split())
    for x in BAN_BUSINESS_TYPES
    if " ".join(str(x).replace(",", " ").split())
}


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


def split_team_tail_id(team: str):
    team = (team or "").strip()
    m = TEAM_TAIL_ID_RE.match(team)
    if not m:
        return team, ""
    base = m.group(1).strip()
    tail_id = m.group(2)
    return (base if base else team), tail_id


def is_all_zeros(s: str) -> bool:
    s = (s or "").strip()
    return bool(s) and set(s) == {"0"}


def sid_rank(sid: str) -> int:
    sid = (sid or "").strip()
    if not sid:
        return 0
    if sid.isdigit() and not is_all_zeros(sid):
        return 3
    if is_all_zeros(sid):
        return 2
    if sid.isdigit():
        return 1
    return 0


def pick_better_sid(a: str, b: str) -> str:
    a = (a or "").strip()
    b = (b or "").strip()
    return b if sid_rank(b) > sid_rank(a) else a


def normalize_team_and_sid(team: str, service_id: str):
    team_base, sid_from_team = split_team_tail_id(team)
    sid = pick_better_sid((service_id or "").strip(), (sid_from_team or "").strip())
    return team_base, sid


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


def is_banned_team(team: str) -> bool:
    t = "" if team is None else str(team).strip()
    if t == "":
        return False
    for x in BAN_TEAMS:
        if x is None:
            continue
        if t == str(x).strip():
            return True
    return False


def read_sd_map(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        log.warning("SD_FILE не найден: %s", path)
        return pd.DataFrame(columns=["code", "sd_name", "owner"])

    df = pd.read_excel(path, sheet_name=0, header=None, dtype=str).fillna("")
    out = pd.DataFrame(
        {
            "code": df.iloc[:, 1].astype(str).str.extract(r"(\d+)", expand=False),
            "sd_name": df.iloc[:, 3].map(clean_spaces),
            "owner": df.iloc[:, 7].map(clean_spaces),
        }
    )
    out = out[out["code"].notna()].copy()
    out["code"] = out["code"].astype(str)
    return out.drop_duplicates(subset=["code"], keep="first")


def load_bk_business_type_map(path: str) -> dict:
    if not path or not os.path.exists(path):
        log.warning("BK_FILE не найден: %s", path)
        return {}

    df = pd.read_excel(path, usecols="A:C,AS", dtype=str).fillna("")
    df.columns = ["c1", "c2", "c3", "business_type"]

    fio = (df["c2"] + " " + df["c1"] + " " + df["c3"]).map(clean_spaces)
    df["fio_key"] = fio.map(normalize_name_key)
    df["business_type"] = df["business_type"].map(clean_spaces)

    df = df[df["fio_key"] != ""].drop_duplicates("fio_key", keep="last")
    return dict(zip(df["fio_key"], df["business_type"]))


def build_matchers(team: str, service_id: str, metric_name: str | None = None) -> str:
    parts = [
        'team!~".+"' if (team or "") == "" else f'team="{team}"',
        'service_id!~".+"' if (service_id or "") == "" else f'service_id="{service_id}"',
    ]
    if metric_name:
        parts.append(f'__name__="{metric_name}"')
    return ", ".join(parts)


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
            service_id = label(m, "service_id")
            metric = label(m, "__name__")

            team_base, sid_norm = normalize_team_and_sid(team, service_id)

            # первая причина — сразу фиксируем и больше эту метрику не трогаем
            if is_banned_team(team_base):
                add_unacc_once("discover", team_base, sid_norm, metric, "banned_team", "team in BAN_TEAMS")
                continue

            if sid_norm and sid_norm in ban_service_set:
                add_unacc_once("discover", team_base, sid_norm, metric, "banned_service_id", "service_id in BAN_SERVICE_IDS")
                continue

            if EXCLUDE_NO_SERVICE_ID_AT_QUERY and not sid_norm:
                add_unacc_once("discover", team_base, sid_norm, metric, "excluded_no_service_id", "EXCLUDE_NO_SERVICE_ID_AT_QUERY=True and service_id empty")
                continue

            if metric:
                metric_keys.add((team_base, sid_norm, metric))

        time.sleep(SLEEP_SEC)

    return sorted(metric_keys)


def samples_48h_for_metric(vm_url: str, metric_name: str, team: str, service_id: str, end_dt: datetime) -> int:
    m = build_matchers(team, service_id, metric_name)
    q = f"sum(count_over_time({{{m}}}[1h]))"
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


def aggregate_to_group(metric_rows):
    # metric_rows: list of {"team","service_id","samples_48h"} per metric
    acc = {}
    for r in metric_rows:
        team_base, sid = normalize_team_and_sid(r.get("team", ""), r.get("service_id", ""))
        samples = int(r.get("samples_48h", 0) or 0)

        key = (team_base, sid)
        if key not in acc:
            acc[key] = {"team": team_base, "service_id": sid, "samples_48h": 0}
        acc[key]["samples_48h"] += samples

    # экстраполяция: (samples_48h / 2 days) * EXTRAPOLATION_DAYS
    out = []
    for (_, _), v in acc.items():
        s48 = int(v["samples_48h"])
        extrap = int(round((s48 / WINDOW_DAYS) * EXTRAPOLATION_DAYS)) if WINDOW_DAYS else 0
        out.append(
            {
                "team": v["team"],
                "service_id": v["service_id"],
                "samples_48h": s48,
                "extrapolation": extrap,
            }
        )
    return out


def enrich_group_rows(group_rows, sd_df: pd.DataFrame, bk_map: dict):
    df = pd.DataFrame(group_rows)
    if df.empty:
        return pd.DataFrame(columns=[
            "team", "service_id", "code", "service_name", "owner_for_report", "business_type",
            "samples_48h", "эксрополяция", "sd_found", "bk_found"
        ])

    df["service_id"] = df["service_id"].astype(str).fillna("").map(lambda x: x.strip())
    df["code"] = df["service_id"].str.extract(r"(\d+)", expand=False).fillna("")
    df.loc[df["code"].isin(list(ban_service_set)), "code"] = ""

    out = df.merge(sd_df, left_on="code", right_on="code", how="left")
    out["sd_found"] = out["sd_name"].fillna("").astype(str).map(lambda x: x.strip() != "")

    out["service_name"] = out["sd_name"].fillna("").astype(str)
    out.loc[out["service_name"] == "", "service_name"] = out["team"]

    out["owner_for_report"] = out["owner"].fillna("").astype(str).map(clean_spaces)

    def _bt(owner: str) -> str:
        owner = clean_spaces(owner)
        return bk_map.get(normalize_name_key(owner), "") if owner else ""

    out["business_type"] = out["owner_for_report"].map(_bt).map(clean_spaces)
    out["bk_found"] = out["business_type"].map(lambda x: x.strip() != "")

    out = out.rename(columns={"extrapolation": "эксрополяция"})
    return out[
        ["team", "service_id", "code", "service_name", "owner_for_report", "business_type",
         "samples_48h", "эксрополяция", "sd_found", "bk_found"]
    ]


def _first_non_empty(vals):
    for v in vals:
        s = "" if v is None else str(v).strip()
        if s:
            return s
    return ""


def dedupe_and_add_percent(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        out = df.copy()
        out["% от общего числа"] = []
        return out.reindex(columns=REPORT_COLS)

    df = df.copy()
    df["КОД"] = df["КОД"].fillna("").astype(str).map(lambda x: x.strip())

    has_code = df[df["КОД"] != ""].copy()
    no_code = df[df["КОД"] == ""].copy()

    if not has_code.empty:
        has_code = has_code.groupby("КОД", as_index=False).agg(
            {
                "Тип бизнеса": _first_non_empty,
                "Наименование сервиса": _first_non_empty,
                "Владелец сервиса": _first_non_empty,
                "samples_48h": "sum",
                "эксрополяция": "sum",
            }
        )

    if not no_code.empty:
        no_code = no_code.groupby("Наименование сервиса", as_index=False).agg(
            {
                "Тип бизнеса": _first_non_empty,
                "КОД": _first_non_empty,
                "Владелец сервиса": _first_non_empty,
                "samples_48h": "sum",
                "эксрополяция": "sum",
            }
        )
        no_code = no_code[
            ["Тип бизнеса", "Наименование сервиса", "КОД", "Владелец сервиса", "samples_48h", "эксрополяция"]
        ]

    out = pd.concat([has_code, no_code], ignore_index=True)

    total = float(out["samples_48h"].sum()) if "samples_48h" in out.columns else 0.0
    out["% от общего числа"] = (out["samples_48h"] / total * 100.0) if total else 0.0
    out["% от общего числа"] = out["% от общего числа"].round(5)

    return out.reindex(columns=REPORT_COLS)


def write_report(df_report: pd.DataFrame, df_unacc: pd.DataFrame):
    wb = Workbook()
    bold = Font(bold=True)

    ws = wb.active
    ws.title = "samples_48h"

    df_report = df_report.reindex(columns=REPORT_COLS)
    ws.append(list(df_report.columns))
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
    if df_unacc is None or df_unacc.empty:
        ws2.append(["No data"])
        ws2["A1"].font = bold
    else:
        df_unacc = df_unacc.reindex(columns=UNACC_COLS)
        ws2.append(list(df_unacc.columns))
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
    log.info("EXCLUDE_NO_SERVICE_ID_AT_QUERY=%s", EXCLUDE_NO_SERVICE_ID_AT_QUERY)
    log.info("EXTRAPOLATION_DAYS=%s", EXTRAPOLATION_DAYS)
    log.info("BAN_SERVICE_IDS=%s", sorted(ban_service_set) if ban_service_set else "[]")
    log.info("SD_FILE=%s", SD_FILE)
    log.info("BK_FILE=%s", BK_FILE)

    sd_df = read_sd_map(SD_FILE)
    bk_map = load_bk_business_type_map(BK_FILE)

    # Unaccounted: строго 1 запись на (team, service_id, metric) — первая причина
    unacc_map = {}  # key -> row

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

    log.info("Discover metrics by (team, service_id, __name__) ...")
    metric_keys = discover_metric_keys(vm_url, add_unacc_once)
    log.info("Metric keys found (eligible): %d", len(metric_keys))
    log.info("Unaccounted (discover): %d", len(unacc_map))

    # карта метрик на группу, чтобы при fail маппинга проставить reason на метрики группы
    per_group_metrics = defaultdict(set)
    for team, sid, mn in metric_keys:
        per_group_metrics[(team, sid)].add(mn)

    end_dt = datetime.now(timezone.utc)

    metric_samples_rows = []
    for idx, (team, service_id, metric) in enumerate(metric_keys, 1):
        key = (team, service_id, metric)
        if key in unacc_map:
            continue  # уже есть причина — не трогаем

        log.info(f"[{idx}/{len(metric_keys)}] team={team} service_id={service_id} metric={metric}")
        try:
            s = samples_48h_for_metric(vm_url, metric, team, service_id, end_dt)
            metric_samples_rows.append({"team": team, "service_id": service_id, "samples_48h": s})
        except Exception as e:
            add_unacc_once("samples", team, service_id, metric, "samples_failed", str(e))
        time.sleep(SLEEP_SEC)

    group_rows = aggregate_to_group(metric_samples_rows)
    enriched = enrich_group_rows(group_rows, sd_df, bk_map)

    if enriched.empty:
        df_report = pd.DataFrame(columns=REPORT_COLS)
    else:
        accounted = enriched.copy()

        # helper: проставить первую причину всем метрикам группы (которые ещё не в unacc)
        def mark_group_metrics_once(team, sid, stage, reason, detail):
            for mn in sorted(per_group_metrics.get((team, sid), set())):
                add_unacc_once(stage, team, sid, mn, reason, detail)

        # sd_not_found: код есть, но в SD нет
        m_sd_missing = (accounted["code"].fillna("").astype(str).str.strip() != "") & (~accounted["sd_found"])
        for r in accounted[m_sd_missing].to_dict("records"):
            mark_group_metrics_once(
                r.get("team", ""),
                r.get("service_id", ""),
                "enrich",
                "sd_not_found",
                "code extracted but not found in SD",
            )

        # owner пустой
        m_owner_empty = accounted["owner_for_report"].map(clean_spaces) == ""
        for r in accounted[m_owner_empty].to_dict("records"):
            mark_group_metrics_once(
                r.get("team", ""),
                r.get("service_id", ""),
                "enrich",
                "owner_empty_in_sd",
                "owner empty after SD merge (no SD match or owner empty in SD)",
            )

        # owner есть, но BK не дал тип бизнеса
        m_bk_missing = (~m_owner_empty) & (accounted["business_type"].map(clean_spaces) == "")
        for r in accounted[m_bk_missing].to_dict("records"):
            mark_group_metrics_once(
                r.get("team", ""),
                r.get("service_id", ""),
                "enrich",
                "owner_not_in_bk",
                "owner present but business_type not found in BK",
            )

        # фильтры: если пустой бизнес тип — выкидываем из отчёта, но метрики помечаем ТОЛЬКО если они ещё не помечены
        if SKIP_EMPTY_BUSINESS_TYPE:
            m = accounted["business_type"].map(clean_spaces) == ""
            for r in accounted[m].to_dict("records"):
                mark_group_metrics_once(
                    r.get("team", ""),
                    r.get("service_id", ""),
                    "filter",
                    "empty_business_type",
                    "SKIP_EMPTY_BUSINESS_TYPE=True and business_type empty",
                )
            accounted = accounted[~m].copy()

        if ban_business_set:
            m = accounted["business_type"].map(clean_spaces).isin(ban_business_set)
            for r in accounted[m].to_dict("records"):
                mark_group_metrics_once(
                    r.get("team", ""),
                    r.get("service_id", ""),
                    "filter",
                    "banned_business_type",
                    "business_type in BAN_BUSINESS_TYPES",
                )
            accounted = accounted[~m].copy()

        df_for_report = accounted.rename(
            columns={
                "business_type": "Тип бизнеса",
                "service_name": "Наименование сервиса",
                "service_id": "КОД",
                "owner_for_report": "Владелец сервиса",
            }
        )[["Тип бизнеса", "Наименование сервиса", "КОД", "Владелец сервиса", "samples_48h", "эксрополяция"]]

        df_report = dedupe_and_add_percent(df_for_report)
        df_report = df_report.sort_values(["samples_48h"], ascending=False).reset_index(drop=True)

    df_unacc = pd.DataFrame(list(unacc_map.values()))
    if not df_unacc.empty:
        df_unacc = df_unacc.reindex(columns=UNACC_COLS).fillna("")
        df_unacc = df_unacc.sort_values(["stage", "reason", "team", "service_id", "metric"]).reset_index(drop=True)

    log.info("Saving report: %s", OUTPUT_FILE)
    write_report(df_report, df_unacc)
    log.info("✔ Done")


if __name__ == "__main__":
    main()