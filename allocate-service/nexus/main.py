# main.py
import os
import logging
import re
import urllib3

from dotenv import load_dotenv
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from humanfriendly import format_size

from nexus_sizes import get_repository_data, get_repository_sizes
from confluence_names import confluence_table_as_dicts, repo_to_service_map


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SKIP_EMPTY_SERVICE = True

BAN_SERVICE_CODES = [
    15473,
]

BAN_BUSINESS_TYPES = []

SKIP_EMPTY_BUSINESS_TYPE = True


def clean_spaces(s):
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_name_key(s):
    return clean_spaces(s).lower()


def split_service_and_code(raw_service):
    s = clean_spaces(raw_service)
    if not s or s in {"-", "—"}:
        return "", ""

    parts = s.split("-")
    if len(parts) >= 2 and parts[-1].isdigit():
        return "-".join(parts[:-1]), parts[-1]

    m = re.search(r"(\d+)$", s)
    if m:
        code = m.group(1)
        name = s[: -len(code)].rstrip("-").strip()
        if name in {"-", "—"}:
            name = ""
        return name, code

    if s in {"-", "—"}:
        return "", ""

    return s, ""


def to_int_bytes(x):
    if x is None:
        return 0
    return int(x)


def build_ban_set(ban_list):
    return {str(x).strip() for x in ban_list if str(x).strip()}


BAN_SET = build_ban_set(BAN_SERVICE_CODES)
BAN_BUSINESS_SET = {clean_spaces(x) for x in BAN_BUSINESS_TYPES if clean_spaces(x)}


def read_sd_map(path):
    if not path or not os.path.exists(path):
        raise RuntimeError(f"SD_FILE не найден: {path}")

    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]

    sd = {}
    for row in ws.iter_rows(values_only=True):
        code_cell = row[1] if len(row) > 1 else ""
        name_cell = row[3] if len(row) > 3 else ""
        owner_cell = row[7] if len(row) > 7 else ""

        code_raw = str(code_cell or "")
        m = re.search(r"(\d+)", code_raw)
        if not m:
            continue
        code = m.group(1)

        sd_name = clean_spaces(str(name_cell or ""))
        owner = clean_spaces(str(owner_cell or ""))

        if code not in sd:
            sd[code] = {"sd_name": sd_name, "owner": owner}

    wb.close()
    return sd


def load_bk_business_type_map(path):
    if not path or not os.path.exists(path):
        return {}

    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]

    out = {}
    for row in ws.iter_rows(values_only=True):
        c1 = clean_spaces(str(row[0] or "")) if len(row) > 0 else ""
        c2 = clean_spaces(str(row[1] or "")) if len(row) > 1 else ""
        c3 = clean_spaces(str(row[2] or "")) if len(row) > 2 else ""
        business_type = clean_spaces(str(row[44] or "")) if len(row) > 44 else ""

        fio = clean_spaces(f"{c2} {c1} {c3}")
        fio_key = normalize_name_key(fio)
        if fio_key:
            out[fio_key] = business_type

    wb.close()
    return out


def write_excel(path, rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "report"

    header = [
        "Тип бизнеса",
        "Наименование сервиса",
        "КОД",
        "Владелец сервиса",
        "Объем",
        "% потребления",
    ]
    ws.append(header)

    bold = Font(bold=True)
    for i in range(1, len(header) + 1):
        ws.cell(row=1, column=i).font = bold

    for r in rows:
        ws.append(
            [
                r["business_type"],
                r["service_name"],
                r["code"],
                r["owner"],
                r["size_human"],
                r["percent"],
            ]
        )

    widths = [len(h) for h in header]
    for r in rows:
        widths[0] = max(widths[0], len(str(r["business_type"])))
        widths[1] = max(widths[1], len(str(r["service_name"])))
        widths[2] = max(widths[2], len(str(r["code"])))
        widths[3] = max(widths[3], len(str(r["owner"])))
        widths[4] = max(widths[4], len(str(r["size_human"])))
        widths[5] = max(widths[5], len(str(r["percent"])))

    for idx, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(idx)].width = min(max(w + 2, 12), 60)

    wb.save(path)


def main():
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    conf_url = os.getenv("CONF_URL", "").strip()
    conf_page_id = os.getenv("CONF_PAGE_ID", "").strip()
    conf_user = os.getenv("CONF_USER", "").strip()
    conf_pass = os.getenv("CONF_PASS", "").strip()

    sd_file = os.getenv("SD_FILE", "sd.xlsx").strip()
    bk_file = os.getenv("BK_FILE", "bk_all_users.xlsx").strip()

    out_file = os.getenv("OUT_FILE", "nexus_service_consumption.xlsx").strip()

    if not conf_url or not conf_page_id or not conf_user or not conf_pass:
        raise RuntimeError("Нужны CONF_URL, CONF_PAGE_ID, CONF_USER, CONF_PASS")

    logging.info("Читаю таблицу из Confluence")
    conf_rows = confluence_table_as_dicts(conf_url, conf_page_id, conf_user, conf_pass)
    repo_service = repo_to_service_map(conf_rows)

    logging.info("Читаю SD и BK")
    sd_map = read_sd_map(sd_file)
    bk_map = load_bk_business_type_map(bk_file)

    logging.info("Читаю репозитории из БД")
    repo_data = get_repository_data()

    logging.info("Считаю размеры репозиториев из БД")
    repo_sizes = get_repository_sizes()

    totals = {}
    hosted_total = 0
    skipped_no_service = 0
    skipped_no_code = 0
    skipped_ban_service_code = 0

    for r in repo_data:
        if (r.get("repository_type") or "").strip().lower() != "hosted":
            continue

        hosted_total += 1
        repo_name = r["repository_name"]

        raw_service = repo_service.get(repo_name, "")
        base_name, code = split_service_and_code(raw_service)

        if SKIP_EMPTY_SERVICE and (not base_name):
            skipped_no_service += 1
            continue

        if not code:
            skipped_no_code += 1
            continue

        if code in BAN_SET:
            skipped_ban_service_code += 1
            continue

        size_bytes = to_int_bytes(repo_sizes.get(repo_name))

        if code not in totals:
            totals[code] = {"size_bytes": 0, "base_name": base_name}
        totals[code]["size_bytes"] += size_bytes

        # на всякий: если в confluence разные base_name для одного code, оставим более длинное
        if base_name and len(base_name) > len(totals[code]["base_name"] or ""):
            totals[code]["base_name"] = base_name

    candidates = []
    skipped_empty_business_type = 0
    skipped_ban_business_type = 0

    for code, v in totals.items():
        size_bytes = v["size_bytes"]
        base_name = v["base_name"]

        sd = sd_map.get(code, {})
        service_name = sd.get("sd_name") or base_name
        owner = sd.get("owner") or ""

        business_type = ""
        if owner:
            business_type = bk_map.get(normalize_name_key(owner), "")
        business_type = clean_spaces(business_type)

        if SKIP_EMPTY_BUSINESS_TYPE and not business_type:
            skipped_empty_business_type += 1
            continue

        if BAN_BUSINESS_SET and business_type in BAN_BUSINESS_SET:
            skipped_ban_business_type += 1
            continue

        candidates.append(
            {
                "business_type": business_type,
                "service_name": service_name,
                "code": code,
                "owner": owner,
                "size_bytes": size_bytes,
            }
        )

    eligible_total = sum(x["size_bytes"] for x in candidates)

    rows = []
    for x in candidates:
        size_bytes = x["size_bytes"]
        percent = 0.0
        if eligible_total > 0:
            percent = (size_bytes / eligible_total) * 100.0

        rows.append(
            {
                "business_type": x["business_type"],
                "service_name": x["service_name"],
                "code": x["code"],
                "owner": x["owner"],
                "size_bytes": size_bytes,
                "size_human": format_size(size_bytes, binary=True),
                "percent": round(percent, 4),
            }
        )

    rows.sort(key=lambda x: x["size_bytes"], reverse=True)

    logging.info(f"hosted repos: {hosted_total}")
    logging.info(f"skipped without service: {skipped_no_service}")
    logging.info(f"skipped without code: {skipped_no_code}")
    logging.info(f"skipped by service code ban: {skipped_ban_service_code}")
    logging.info(f"skipped empty business type: {skipped_empty_business_type}")
    logging.info(f"skipped by business type ban: {skipped_ban_business_type}")
    logging.info(f"services in report: {len(rows)}")
    logging.info(f"eligible_total: {format_size(eligible_total, binary=True)}")
    logging.info(f"write excel: {out_file}")

    write_excel(out_file, rows)

    logging.info("done")


if __name__ == "__main__":
    main()
