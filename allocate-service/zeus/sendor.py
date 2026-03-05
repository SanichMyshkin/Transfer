import os
import re
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict

from dotenv import load_dotenv
import psycopg2
from openpyxl import Workbook, load_workbook

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

SD_FILE = os.getenv("SD_FILE", "sd.xlsx")
BK_FILE = os.getenv("BK_FILE", "bk_all_users.xlsx")

EXCLUDE_SERVICE_IDS = {
    "15473",
}

ALLOW_ZERO_SERVICE_ID = False

SKIP_EMPTY_BUSINESS_TYPE = False
SKIP_SD_MAPPING_MISS = False


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


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


def read_sd_map(path: str):
    if not path or not os.path.exists(path):
        log.warning(f"SD_FILE не найден: {path}")
        return {}

    wb = load_workbook(path, data_only=True)
    ws = wb.worksheets[0]

    out = {}
    rows = 0
    ok = 0

    for r in ws.iter_rows(values_only=True):
        rows += 1

        code_raw = r[1] if len(r) > 1 else ""
        sd_name = r[3] if len(r) > 3 else ""
        owner = r[7] if len(r) > 7 else ""

        m = re.search(r"(\d+)", str(code_raw or ""))
        code = m.group(1) if m else ""
        if not code:
            continue

        out[str(code)] = {
            "sd_name": clean_spaces(sd_name),
            "owner": clean_spaces(owner),
        }
        ok += 1

    log.info(f"SD: rows={rows} mapped_codes={len(out)} ok_rows={ok}")
    return out


def load_bk_business_type_map(path: str):
    if not path or not os.path.exists(path):
        log.warning(f"BK_FILE не найден: {path}")
        return {}

    wb = load_workbook(path, data_only=True)
    ws = wb.worksheets[0]

    out = {}
    rows = 0
    ok = 0

    for r in ws.iter_rows(values_only=True):
        rows += 1

        c1 = r[0] if len(r) > 0 else ""
        c2 = r[1] if len(r) > 1 else ""
        c3 = r[2] if len(r) > 2 else ""

        if len(r) <= 44:
            continue
        bt = r[44]

        fio = clean_spaces(f"{c2} {c1} {c3}")
        fio_key = normalize_name_key(fio)
        bt = clean_spaces(bt)

        if not fio_key:
            continue

        out[fio_key] = bt
        ok += 1

    log.info(f"BK: rows={rows} mapped_fio={len(out)} ok_rows={ok}")
    return out


def aggregate_and_enrich(db_rows, exclude_service_ids, sd_map, bk_map):
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

    total_included = sum(include_counts.values())

    rows_main = []
    for sid, cnt in sorted(include_counts.items(), key=lambda x: x[1], reverse=True):
        sd = sd_map.get(str(sid), {})  # service_id == SD "КОД"
        sd_name = clean_spaces(sd.get("sd_name", ""))
        owner = clean_spaces(sd.get("owner", ""))

        business_type = ""
        if owner:
            business_type = clean_spaces(bk_map.get(normalize_name_key(owner), ""))

        if SKIP_SD_MAPPING_MISS and not sd_name and not owner:
            unaccounted_rows.append([sid, "", "", "", cnt, "sd_mapping_miss"])
            continue

        if SKIP_EMPTY_BUSINESS_TYPE and not business_type:
            unaccounted_rows.append([sid, sd_name, owner, business_type, cnt, "missing_business_type"])
            continue

        service_name = sd_name if sd_name else sid
        pct = 0.0 if total_included <= 0 else (cnt * 100.0 / total_included)

        rows_main.append([sid, business_type, service_name, owner, cnt, pct])

    return rows_main, unaccounted_rows, total_included


def write_excel(path, rows_main, unaccounted_rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "by_service"

    ws.append(["service_id", "business_type", "service_name", "owner", "messages_count", "percent_of_all_total"])
    for r in rows_main:
        ws.append(r)

    ws2 = wb.create_sheet("unaccounted")
    ws2.append(["service_id", "service_name", "owner", "business_type", "messages_count", "reason"])

    ua_sorted = sorted(
        unaccounted_rows,
        key=lambda r: (str(r[5]), -int(r[4] or 0), str(r[0])),
    )
    for r in ua_sorted:
        ws2.append(r)

    wb.save(path)
    log.info(f"XLSX saved {path}")


def main():
    log.info("START")
    log.info(f"SINCE_DAYS={SINCE_DAYS}")
    log.info(f"EXCLUDE_SERVICE_IDS={sorted(EXCLUDE_SERVICE_IDS)}")
    log.info(f"ALLOW_ZERO_SERVICE_ID={ALLOW_ZERO_SERVICE_ID}")
    log.info(f"SKIP_EMPTY_BUSINESS_TYPE={SKIP_EMPTY_BUSINESS_TYPE}")
    log.info(f"SKIP_SD_MAPPING_MISS={SKIP_SD_MAPPING_MISS}")
    log.info(f"SD_FILE={SD_FILE}")
    log.info(f"BK_FILE={BK_FILE}")

    sd_map = read_sd_map(SD_FILE)
    bk_map = load_bk_business_type_map(BK_FILE)

    # Реальный запрос пока выключен
    # db_rows = get_counts_by_service_since_days_sql(SINCE_DAYS)
    # Фикстура мужики, рассходимся
    db_rows = get_counts_fixture(SINCE_DAYS)

    rows_main, unaccounted_rows, total_included = aggregate_and_enrich(
        db_rows=db_rows,
        exclude_service_ids=EXCLUDE_SERVICE_IDS,
        sd_map=sd_map,
        bk_map=bk_map,
    )

    log.info(f"RESULT: main_rows={len(rows_main)} unaccounted_rows={len(unaccounted_rows)} total_included={total_included}")

    write_excel(OUT_XLSX, rows_main, unaccounted_rows)
    log.info("DONE")


if __name__ == "__main__":
    main()