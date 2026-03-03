import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
import re

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

BAN_TEAMS = [
    "UNAITP",
]

BAN_SERVICE_IDS = [15473]

BAN_BUSINESS_TYPES = []

SKIP_EMPTY_BUSINESS_TYPE = True

EXCLUDE_NO_SERVICE_ID_AT_QUERY = False
EXTRAPOLATION_DAYS = 90

SD_FILE = os.getenv("SD_FILE")
BK_FILE = os.getenv("BK_FILE")

TEAM_TAIL_ID_RE = re.compile(r"^(.*)-(\d+)$")

WINDOW_HOURS = 72  # было 24
WINDOW_DAYS = WINDOW_HOURS / 24.0  # 3.0

REPORT_COLS = [
    "Тип бизнеса",
    "Наименование сервиса",
    "КОД",
    "Владелец сервиса",
    "samples_72h",
    "эксрополяция",
    "% от общего числа",
]


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
    log.info(f"QUERY_RANGE: {query}")
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


def discover_groups(vm_url: str):
    groups = set()
    unacc = []

    def add_unacc(team, service_id, reason, detail):
        team_base, sid_norm = normalize_team_and_sid(team, service_id)
        unacc.append(
            {
                "stage": "discover_groups",
                "team": team_base,
                "service_id": sid_norm,
                "reason": reason,
                "detail": detail,
            }
        )

    queries = [
        'count by (team, service_id) ({team=~".+", service_id=~".+"})',
        'count by (team, service_id) ({team!~".+", service_id=~".+"})',
        'count by (team, service_id) ({team=~".+", service_id!~".+"})',
        'count by (team, service_id) ({team!~".+", service_id!~".+"})',
    ]
    for q in queries:
        rows = http_query(vm_url, q)
        for r in rows:
            m = r.get("metric", {}) or {}
            team = label(m, "team")
            service_id = label(m, "service_id")

            if is_banned_team(team):
                add_unacc(team, service_id, "banned_team", "team in BAN_TEAMS")
                continue

            team_base, sid_norm = normalize_team_and_sid(team, service_id)

            if sid_norm and sid_norm in ban_service_set:
                add_unacc(team_base, sid_norm, "banned_service_id", "service_id in BAN_SERVICE_IDS")
                continue

            if EXCLUDE_NO_SERVICE_ID_AT_QUERY and not sid_norm:
                add_unacc(team_base, sid_norm, "excluded_no_service_id", "EXCLUDE_NO_SERVICE_ID_AT_QUERY=True and service_id empty")
                continue

            groups.add((team_base, sid_norm))

        time.sleep(SLEEP_SEC)

    return sorted(groups), unacc


def build_matchers(team: str, service_id: str) -> str:
    return ", ".join(
        [
            'team!~".+"' if (team or "") == "" else f'team="{team}"',
            'service_id!~".+"' if (service_id or "") == "" else f'service_id="{service_id}"',
        ]
    )


def get_group_metric_names(vm_url: str, team: str, service_id: str) -> list[str]:
    m = build_matchers(team, service_id)
    rows = http_query(vm_url, f"count by (__name__) ({{{m}}})")
    names = []
    for r in rows or []:
        mm = r.get("metric", {}) or {}
        n = mm.get("__name__")
        if n is None:
            continue
        s = str(n).strip()
        if s:
            names.append(s)
    return sorted(set(names))


def samples_72h_for_metric_in_group(vm_url: str, metric_name: str, team: str, service_id: str, end_dt: datetime) -> int:
    m = build_matchers(team, service_id)
    q = f'sum(count_over_time({{{m}, __name__="{metric_name}"}}[1h]))'
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


def normalize_out_rows(rows):
    acc = {}
    for r in rows:
        team_base, sid = normalize_team_and_sid(r.get("team", ""), r.get("service_id", ""))
        samples = int(r.get("samples_72h", 0) or 0)

        if team_base not in acc:
            acc[team_base] = {
                "team": team_base,
                "service_id": sid,
                "samples_72h": samples,
                "extrapolation": int(round((samples / WINDOW_DAYS) * EXTRAPOLATION_DAYS)) if WINDOW_DAYS else 0,
            }
        else:
            acc[team_base]["samples_72h"] += samples
            acc[team_base]["extrapolation"] += int(round((samples / WINDOW_DAYS) * EXTRAPOLATION_DAYS)) if WINDOW_DAYS else 0
            acc[team_base]["service_id"] = pick_better_sid(acc[team_base]["service_id"], sid)

    return list(acc.values())


def enrich_with_sd_and_bk(rows, sd_df: pd.DataFrame, bk_map: dict) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "team",
                "service_id",
                "code",
                "service_name",
                "owner_for_report",
                "business_type",
                "samples_72h",
                "эксрополяция",
                "sd_found",
                "bk_found",
            ]
        )

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
        [
            "team",
            "service_id",
            "code",
            "service_name",
            "owner_for_report",
            "business_type",
            "samples_72h",
            "эксрополяция",
            "sd_found",
            "bk_found",
        ]
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
                "samples_72h": "sum",
                "эксрополяция": "sum",
            }
        )

    if not no_code.empty:
        no_code = no_code.groupby("Наименование сервиса", as_index=False).agg(
            {
                "Тип бизнеса": _first_non_empty,
                "КОД": _first_non_empty,
                "Владелец сервиса": _first_non_empty,
                "samples_72h": "sum",
                "эксрополяция": "sum",
            }
        )
        no_code = no_code[
            [
                "Тип бизнеса",
                "Наименование сервиса",
                "КОД",
                "Владелец сервиса",
                "samples_72h",
                "эксрополяция",
            ]
        ]

    out = pd.concat([has_code, no_code], ignore_index=True)

    total = float(out["samples_72h"].sum()) if "samples_72h" in out.columns else 0.0
    out["% от общего числа"] = (out["samples_72h"] / total * 100.0) if total else 0.0
    out["% от общего числа"] = out["% от общего числа"].round(5)

    return out.reindex(columns=REPORT_COLS)


def write_report(df_report: pd.DataFrame, df_unacc: pd.DataFrame):
    wb = Workbook()
    bold = Font(bold=True)

    ws = wb.active
    ws.title = "samples_72h"

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
        ws2.append(list(df_unacc.columns))
        for c in ws2[1]:
            c.font = bold
        for row in df_unacc.itertuples(index=False):
            ws2.append(list(row))

    wb.save(OUTPUT_FILE)


def main():
    load_dotenv()
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

    log.info("Собираю группы (team / service_id)...")
    groups, unacc = discover_groups(vm_url)
    log.info(f"Групп найдено: {len(groups)}")

    end_dt = datetime.now(timezone.utc)

    def add_unacc(team, service_id, reason, detail, stage="calc/enrich"):
        team_base, sid_norm = normalize_team_and_sid(team, service_id)
        unacc.append(
            {
                "stage": stage,
                "team": team_base,
                "service_id": sid_norm,
                "reason": reason,
                "detail": detail,
            }
        )

    out_rows = []
    for team, service_id in groups:
        log.info("[GROUP] " + (f"team={team} service_id={service_id}" if (team or service_id) else "UNLABELED"))

        try:
            metric_names = get_group_metric_names(vm_url, team, service_id)
        except Exception as e:
            log.error(f"Не смог получить метрики для группы team={team} service_id={service_id}: {e}")
            add_unacc(team, service_id, "metric_names_failed", str(e), stage="metric_names")
            continue

        time.sleep(SLEEP_SEC)

        group_total = 0
        for i, mn in enumerate(metric_names, 1):
            log.info(f"  metric[{i}/{len(metric_names)}]={mn}")
            try:
                s = samples_72h_for_metric_in_group(vm_url, mn, team, service_id, end_dt)
                group_total += s
            except requests.exceptions.HTTPError as e:
                log.error(f"  HTTP error metric={mn} team={team} service_id={service_id}: {e}")
            except Exception as e:
                log.error(f"  Ошибка metric={mn} team={team} service_id={service_id}: {e}")
            time.sleep(SLEEP_SEC)

        out_rows.append({"team": team, "service_id": service_id, "samples_72h": group_total})

    out_rows = normalize_out_rows(out_rows)

    enriched = enrich_with_sd_and_bk(out_rows, sd_df, bk_map)
    if enriched.empty:
        df_report = pd.DataFrame(columns=REPORT_COLS)
    else:
        accounted = enriched.copy()

        # маппинг в SD
        m_sd_missing = (accounted["code"].fillna("").astype(str).str.strip() != "") & (~accounted["sd_found"])
        if m_sd_missing.any():
            for r in accounted[m_sd_missing].to_dict("records"):
                add_unacc(
                    r.get("team", ""),
                    r.get("service_id", ""),
                    "sd_not_found",
                    "code extracted but not found in SD",
                    stage="enrich",
                )

        # owner пустой после SD
        m_owner_empty = accounted["owner_for_report"].map(clean_spaces) == ""
        if m_owner_empty.any():
            for r in accounted[m_owner_empty].to_dict("records"):
                add_unacc(
                    r.get("team", ""),
                    r.get("service_id", ""),
                    "owner_empty_in_sd",
                    "owner empty after SD merge (no SD match or owner empty in SD)",
                    stage="enrich",
                )

        # BK не нашёл business_type
        m_bk_missing = (~m_owner_empty) & (accounted["business_type"].map(clean_spaces) == "")
        if m_bk_missing.any():
            for r in accounted[m_bk_missing].to_dict("records"):
                add_unacc(
                    r.get("team", ""),
                    r.get("service_id", ""),
                    "owner_not_in_bk",
                    "owner present but business_type not found in BK",
                    stage="enrich",
                )

        # фильтр по business_type => в unaccounted и убрать из отчёта
        if SKIP_EMPTY_BUSINESS_TYPE:
            m = accounted["business_type"].map(clean_spaces) == ""
            if m.any():
                for r in accounted[m].to_dict("records"):
                    add_unacc(
                        r.get("team", ""),
                        r.get("service_id", ""),
                        "empty_business_type",
                        "SKIP_EMPTY_BUSINESS_TYPE=True and business_type empty",
                        stage="filter",
                    )
            accounted = accounted[~m].copy()

        if ban_business_set:
            m = accounted["business_type"].map(clean_spaces).isin(ban_business_set)
            if m.any():
                for r in accounted[m].to_dict("records"):
                    add_unacc(
                        r.get("team", ""),
                        r.get("service_id", ""),
                        "banned_business_type",
                        "business_type in BAN_BUSINESS_TYPES",
                        stage="filter",
                    )
            accounted = accounted[~m].copy()

        df_for_report = accounted.rename(
            columns={
                "business_type": "Тип бизнеса",
                "service_name": "Наименование сервиса",
                "service_id": "КОД",
                "owner_for_report": "Владелец сервиса",
            }
        )[["Тип бизнеса", "Наименование сервиса", "КОД", "Владелец сервиса", "samples_72h", "эксрополяция"]]

        df_report = dedupe_and_add_percent(df_for_report)
        df_report = df_report.sort_values(["samples_72h"], ascending=False).reset_index(drop=True)

    df_unacc = pd.DataFrame(unacc)
    if not df_unacc.empty:
        df_unacc = df_unacc.drop_duplicates(subset=["stage", "team", "service_id", "reason", "detail"])
        df_unacc = df_unacc.sort_values(["stage", "reason", "team"], ascending=[True, True, True]).reset_index(drop=True)

    log.info(f"Сохраняю отчет: {OUTPUT_FILE}")
    write_report(df_report, df_unacc)
    log.info("✔ Готово")


if __name__ == "__main__":
    main()