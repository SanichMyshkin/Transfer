#!/usr/bin/env python3
import os
import sys
import logging
import re

import clickhouse_connect
import humanize
import pandas as pd
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font

load_dotenv()

HOST = os.getenv("CH_HOST")
PORT = int(os.getenv("CH_PORT", 8123))
USER = os.getenv("CH_USER", "default")
PASSWORD = os.getenv("CH_PASSWORD", "")
DATABASE = os.getenv("CH_DATABASE")
SECURE = False
VERIFY = False
OUTPUT_FILE = os.getenv("CH_OUT", "openTelemetry_report.xlsx")

SD_FILE = os.getenv("SD_FILE", "sd.xlsx")
BK_FILE = os.getenv("BK_FILE", "bk_all_users.xlsx")

LOG = logging.getLogger("ch_table_sizes")

SQL = """
SELECT
    t.name AS table,
    coalesce(sum(p.bytes_on_disk), 0) AS bytes_on_disk
FROM system.tables AS t
LEFT JOIN system.parts AS p
    ON p.database = t.database
   AND p.table = t.name
   AND p.active
WHERE t.database = {db:String}
  AND t.engine NOT IN ('View', 'MaterializedView', 'LiveView')
  AND t.engine != 'Distributed'
GROUP BY table
ORDER BY bytes_on_disk DESC
"""

TABLE_PREFIX_OVERRIDES = {
    # "otel_traces": ("SOMESVC", 12345),
}

NAME_RE = re.compile(r"^otel_([a-z0-9]+)_(\d+)_traces(?:_trace_id_ts)?$", re.IGNORECASE)


def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def die(msg: str, code: int = 2):
    LOG.error(msg)
    raise SystemExit(code)


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


def normalize_number(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".", 1)[0]
    return s


def load_bk_business_type_map(path: str):
    if not path or not os.path.isfile(path):
        die(f"BK_FILE не найден: {path}")

    df = pd.read_excel(path, usecols="A:C,AS", dtype=str, engine="openpyxl").fillna("")
    df.columns = ["c1", "c2", "c3", "business_type"]

    def make_fio(r):
        fio = " ".join([clean_spaces(r["c2"]), clean_spaces(r["c1"]), clean_spaces(r["c3"])])
        return clean_spaces(fio)

    df["fio_key"] = df.apply(make_fio, axis=1).map(normalize_name_key)
    df["business_type"] = df["business_type"].astype(str).map(clean_spaces)

    last = df[df["fio_key"] != ""].drop_duplicates("fio_key", keep="last")
    mp = dict(zip(last["fio_key"], last["business_type"]))

    LOG.info("BK: загружено ФИО → тип бизнеса: %d", len(mp))
    return mp


def load_sd_mapping(path: str):
    if not path or not os.path.isfile(path):
        die(f"SD_FILE не найден: {path}")

    df = pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    map_by_number = {}

    for _, row in df.iterrows():
        num = normalize_number(row.iloc[1] if len(row) > 1 else "")
        sd_name_raw = row.iloc[3] if len(row) > 3 else ""
        owner_raw = row.iloc[7] if len(row) > 7 else ""

        sd_name = clean_spaces(sd_name_raw)
        owner = clean_spaces(owner_raw)

        payload = {"sd_name": sd_name, "owner": owner}

        if num:
            map_by_number[num] = payload

    LOG.info("SD: загружено сервисов по номеру=%d", len(map_by_number))
    return map_by_number


def map_table(table_name: str):
    for base, (svc_name, svc_id) in TABLE_PREFIX_OVERRIDES.items():
        if table_name == base or table_name == f"{base}_trace_id_ts":
            return svc_name, int(svc_id), "override"

    m = NAME_RE.match(table_name)
    if not m:
        return None, None, "unmatched"

    return m.group(1).upper(), int(m.group(2)), "parsed"


def resolve_sd_bk(service_id: int, sd_map_by_number, bk_type_map):
    sid = str(service_id)
    sd = sd_map_by_number.get(sid) or sd_map_by_number.get(normalize_number(sid) or "")
    sd_name = (sd or {}).get("sd_name") or ""
    owner = (sd or {}).get("owner") or ""
    business_type = bk_type_map.get(normalize_name_key(owner), "") if owner else ""
    return sd_name, owner, business_type


def main() -> int:
    setup_logging()

    if not HOST or not DATABASE:
        LOG.error("CH_HOST and CH_DATABASE must be set")
        return 1

    sd_map_by_number = load_sd_mapping(SD_FILE)
    bk_type_map = load_bk_business_type_map(BK_FILE)

    LOG.info("Connecting to %s:%s secure=%s", HOST, PORT, SECURE)

    try:
        client = clickhouse_connect.get_client(
            host=HOST,
            port=PORT,
            username=USER,
            password=PASSWORD,
            secure=SECURE,
            verify=VERIFY,
        )
    except Exception:
        LOG.exception("Connection failed")
        return 2

    try:
        LOG.info("Executing query for database=%s", DATABASE)
        res = client.query(SQL, parameters={"db": DATABASE})
        table_rows = res.result_rows
        LOG.info("Query returned %d tables (including empty)", len(table_rows))
    except Exception:
        LOG.exception("Query failed")
        return 3

    agg = {}
    unmatched = 0
    unaccounted_rows = []

    for table, bytes_on_disk in table_rows:
        b = int(bytes_on_disk or 0)

        parsed_service_name, service_id, src = map_table(table)
        if src == "unmatched" or service_id is None:
            unmatched += 1
            LOG.warning("Unmatched table (not accounted): %s (bytes=%d)", table, b)
            unaccounted_rows.append(
                {
                    "table": table,
                    "bytes_on_disk": b,
                    "size_h": humanize.naturalsize(b, binary=True),
                    "reason": "unmatched",
                    "detail": "table name does not match NAME_RE and no TABLE_PREFIX_OVERRIDES hit",
                    "service_id": "",
                    "sd_name": "",
                    "owner": "",
                    "business_type": "",
                }
            )
            continue

        sd_name, owner, business_type = resolve_sd_bk(service_id, sd_map_by_number, bk_type_map)

        if service_id not in agg:
            agg[service_id] = {
                "service_id": int(service_id),
                "sd_name": sd_name,
                "owner": owner,
                "business_type": business_type,
                "bytes": 0,
            }
        else:
            if not agg[service_id]["sd_name"] and sd_name:
                agg[service_id]["sd_name"] = sd_name
            if not agg[service_id]["owner"] and owner:
                agg[service_id]["owner"] = owner
            if not agg[service_id]["business_type"] and business_type:
                agg[service_id]["business_type"] = business_type

        agg[service_id]["bytes"] += b

        if src == "override" and not agg[service_id]["sd_name"] and parsed_service_name:
            agg[service_id]["sd_name"] = parsed_service_name

    total_accounted = sum(v["bytes"] for v in agg.values())

    wb = Workbook()

    ws = wb.active
    ws.title = "По сервисам"

    headers = [
        "Тип бизнеса",
        "Наименование сервиса",
        "КОД",
        "Владелец",
        "Объем (bytes)",
        "Объем",
        "% от учтенного итога",
    ]
    ws.append(headers)

    bold = Font(bold=True)
    for i in range(1, len(headers) + 1):
        ws.cell(row=1, column=i).font = bold

    items = sorted(
        ((sid, v["sd_name"], v["owner"], v["business_type"], v["bytes"]) for sid, v in agg.items()),
        key=lambda x: x[4],
        reverse=True,
    )

    for service_id, sd_name, owner, business_type, bytes_sum in items:
        pct = (bytes_sum / total_accounted * 100.0) if total_accounted > 0 else 0.0
        ws.append(
            [
                business_type,
                sd_name,
                int(service_id),
                owner,
                int(bytes_sum),
                humanize.naturalsize(bytes_sum, binary=True),
                round(pct, 6),
            ]
        )

    ws2 = wb.create_sheet("Неучтенные таблицы")
    headers2 = [
        "Таблица",
        "Объем (bytes)",
        "Объем",
        "Причина",
        "Детали",
    ]
    ws2.append(headers2)
    for i in range(1, len(headers2) + 1):
        ws2.cell(row=1, column=i).font = bold

    for r in unaccounted_rows:
        ws2.append(
            [
                r.get("table", ""),
                int(r.get("bytes_on_disk") or 0),
                r.get("size_h", ""),
                r.get("reason", ""),
                r.get("detail", ""),
            ]
        )

    wb.save(OUTPUT_FILE)

    LOG.info(
        "Report saved: %s | services=%d | tables_total=%d | unmatched_tables=%d | unaccounted_rows=%d | total_accounted=%s",
        OUTPUT_FILE,
        len(agg),
        len(table_rows),
        unmatched,
        len(unaccounted_rows),
        humanize.naturalsize(total_accounted, binary=True),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())