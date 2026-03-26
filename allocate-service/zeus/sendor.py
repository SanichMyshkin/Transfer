import os
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
import psycopg2
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

SINCE_DAYS = int(os.getenv("SINCE", "90"))
OUT_XLSX = os.getenv("OUT_XLSX", "sendor_report.xlsx")

ACTIVITY_FILE = os.getenv("ACTIVITY_FILE", "activity.xlsx")

EXCLUDE_SERVICE_IDS = {
    "15473",
}

ALLOW_ZERO_SERVICE_ID = False
SKIP_ACTIVITY_MAPPING_MISS = False


def clean_spaces(s) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_code(v):
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return str(int(v))
    s = str(v).strip()
    return s[:-2] if s.endswith(".0") and s[:-2].isdigit() else s


def is_all_zeros(s: str) -> bool:
    return bool(s) and set(s) == {"0"}


def get_counts_by_service_since_days_sql(days: int):
    since_dt = datetime.now(timezone.utc) - timedelta(days=days)

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.set_session(readonly=True)

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select
                        nullif((metadata->>'service_id')::text, '')::bigint as service_id,
                        count(*) as cnt
                    from sender.telegram_events_history
                    where created >= %s
                    group by 1
                    """,
                    (since_dt,),
                )
                return cur.fetchall()
    finally:
        conn.close()


def get_counts_fixture(days: int):
    rows = [
        (11203, 1740),
        (99999, 321),
        (5531, 10),
        (15473, 777),
        (0, 50),
        (None, 77),
    ]
    log.info("FIXTURE используется")
    return rows


def read_activity_map(path: str):
    if not path or not os.path.exists(path):
        log.warning("ACTIVITY_FILE не найден: %s", path)
        return {}

    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]

    out = {}
    rows = 0
    ok = 0

    for r in ws.iter_rows(values_only=True):
        rows += 1

        code = normalize_code(r[0] if len(r) > 0 else "")
        if not code:
            continue

        if code in out:
            continue

        out[code] = {
            "service_name": clean_spaces(r[1] if len(r) > 1 else ""),
            "activity_code": clean_spaces(r[2] if len(r) > 2 else ""),
            "activity_name": clean_spaces(r[3] if len(r) > 3 else ""),
        }
        ok += 1

    wb.close()
    log.info("ACTIVITY: rows=%d mapped_codes=%d ok_rows=%d", rows, len(out), ok)
    return out


def aggregate_and_enrich(db_rows, exclude_service_ids, activity_map):
    include_counts = defaultdict(int)
    unaccounted_rows = []

    for service_id, cnt in db_rows:
        c = int(cnt or 0)

        if service_id is None:
            unaccounted_rows.append(["", "", "", "", c, "missing_service_id"])
            continue

        sid = str(service_id).strip()
        if not sid:
            unaccounted_rows.append(["", "", "", "", c, "missing_service_id"])
            continue

        if is_all_zeros(sid) and not ALLOW_ZERO_SERVICE_ID:
            unaccounted_rows.append([sid, "", "", "", c, "zero_service_id"])
            continue

        if sid in exclude_service_ids:
            unaccounted_rows.append([sid, "", "", "", c, "excluded_by_config"])
            continue

        include_counts[sid] += c

    rows_main = []
    rows_for_percent = []
    skipped_activity_miss = 0

    for sid, cnt in sorted(include_counts.items(), key=lambda x: x[1], reverse=True):
        meta = activity_map.get(str(sid), {})
        service_name = clean_spaces(meta.get("service_name", ""))
        activity_code = clean_spaces(meta.get("activity_code", ""))
        activity_name = clean_spaces(meta.get("activity_name", ""))

        if SKIP_ACTIVITY_MAPPING_MISS and not service_name:
            skipped_activity_miss += 1
            unaccounted_rows.append(
                [sid, "", "", "", cnt, "activity_mapping_miss"]
            )
            continue

        rows_for_percent.append(cnt)
        rows_main.append(
            {
                "service_id": sid,
                "service_name": service_name or sid,
                "activity_code": activity_code,
                "activity_name": activity_name,
                "messages_count": int(cnt),
                "percent_of_all_total": 0.0,
            }
        )

    total_included = sum(rows_for_percent)

    for row in rows_main:
        cnt = int(row["messages_count"] or 0)
        pct = 0.0 if total_included <= 0 else (cnt * 100.0 / total_included)
        row["percent_of_all_total"] = pct

    log.info(
        "aggregate_and_enrich: included_before=%d main_rows=%d skipped_activity_miss=%d unaccounted=%d total_included=%d",
        len(include_counts),
        len(rows_main),
        skipped_activity_miss,
        len(unaccounted_rows),
        total_included,
    )

    return rows_main, unaccounted_rows, total_included


def write_excel(path, rows_main, unaccounted_rows):
    wb = Workbook()
    bold = Font(bold=True)

    ws = wb.active
    ws.title = "by_service"

    headers = [
        "service_id",
        "service_name",
        "activity_code",
        "activity_name",
        "messages_count",
        "percent_of_all_total",
    ]
    ws.append(headers)
    for c in ws[1]:
        c.font = bold

    for r in rows_main:
        ws.append(
            [
                r["service_id"],
                r["service_name"],
                r["activity_code"],
                r["activity_name"],
                int(r["messages_count"]),
                float(r["percent_of_all_total"]),
            ]
        )

    pct_col = headers.index("percent_of_all_total") + 1
    for rr in range(2, ws.max_row + 1):
        ws.cell(row=rr, column=pct_col).number_format = "0.00000"

    ws2 = wb.create_sheet("unaccounted")
    headers2 = [
        "service_id",
        "service_name",
        "activity_code",
        "activity_name",
        "messages_count",
        "reason",
    ]
    ws2.append(headers2)
    for c in ws2[1]:
        c.font = bold

    ua_sorted = sorted(
        unaccounted_rows,
        key=lambda r: (str(r[5]), -int(r[4] or 0), str(r[0])),
    )
    for r in ua_sorted:
        ws2.append(r)

    wb.save(path)
    log.info("XLSX saved %s", path)


def main():
    log.info("START")
    log.info("SINCE_DAYS=%s", SINCE_DAYS)
    log.info("EXCLUDE_SERVICE_IDS=%s", sorted(EXCLUDE_SERVICE_IDS))
    log.info("ALLOW_ZERO_SERVICE_ID=%s", ALLOW_ZERO_SERVICE_ID)
    log.info("SKIP_ACTIVITY_MAPPING_MISS=%s", SKIP_ACTIVITY_MAPPING_MISS)
    log.info("ACTIVITY_FILE=%s", ACTIVITY_FILE)

    activity_map = read_activity_map(ACTIVITY_FILE)

    # db_rows = get_counts_by_service_since_days_sql(SINCE_DAYS)
    db_rows = get_counts_fixture(SINCE_DAYS)

    rows_main, unaccounted_rows, total_included = aggregate_and_enrich(
        db_rows=db_rows,
        exclude_service_ids=EXCLUDE_SERVICE_IDS,
        activity_map=activity_map,
    )

    log.info(
        "RESULT: main_rows=%d unaccounted_rows=%d total_included=%d",
        len(rows_main),
        len(unaccounted_rows),
        total_included,
    )

    write_excel(OUT_XLSX, rows_main, unaccounted_rows)
    log.info("DONE")


if __name__ == "__main__":
    main()