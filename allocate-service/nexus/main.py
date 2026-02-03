import os
import logging
from dotenv import load_dotenv

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

from humanfriendly import format_size

from nexus_sizes import get_repository_data, get_repository_sizes
from confluence_names import confluence_table_as_dicts, repo_to_service_map


SKIP_EMPTY_SERVICE = True


def write_excel(path, rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "report"

    header = ["service_name", "code", "size_human", "percent_consumption"]
    ws.append(header)

    bold = Font(bold=True)
    for i in range(1, len(header) + 1):
        ws.cell(row=1, column=i).font = bold

    for r in rows:
        ws.append([
            r["service_name"],
            r["code"],
            r["size_human"],
            r["percent_consumption"],
        ])

    widths = [len(h) for h in header]
    for r in rows:
        widths[0] = max(widths[0], len(str(r["service_name"])))
        widths[1] = max(widths[1], len(str(r["code"])))
        widths[2] = max(widths[2], len(str(r["size_human"])))
        widths[3] = max(widths[3], len(str(r["percent_consumption"])))

    for idx, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(idx)].width = min(max(w + 2, 12), 60)

    wb.save(path)


def split_service_and_code(raw_service):
    s = (raw_service or "").strip()
    if not s:
        return "", ""

    parts = s.split("-")
    if len(parts) >= 2 and parts[-1].isdigit():
        return "-".join(parts[:-1]), parts[-1]

    return s, ""


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

    out_file = os.getenv("OUT_FILE", "nexus_service_consumption.xlsx")

    if not conf_url or not conf_page_id or not conf_user or not conf_pass:
        raise RuntimeError("Нужны CONF_URL, CONF_PAGE_ID, CONF_USER, CONF_PASS")

    logging.info("Читаю таблицу из Confluence")
    conf_rows = confluence_table_as_dicts(conf_url, conf_page_id, conf_user, conf_pass)
    repo_service = repo_to_service_map(conf_rows)

    logging.info("Читаю репозитории из БД")
    repo_data = get_repository_data()

    logging.info("Считаю размеры репозиториев из БД")
    repo_sizes = get_repository_sizes()

    totals_bytes = {}

    hosted_total = 0
    skipped_no_service = 0

    for r in repo_data:
        if (r.get("repository_type") or "").strip().lower() != "hosted":
            continue

        hosted_total += 1
        repo_name = r["repository_name"]

        raw_service = repo_service.get(repo_name, "")
        if not raw_service:
            if SKIP_EMPTY_SERVICE:
                skipped_no_service += 1
                continue
            raw_service = "unknown"

        size_bytes = repo_sizes.get(repo_name, 0) or 0
        totals_bytes[raw_service] = totals_bytes.get(raw_service, 0) + size_bytes

    grand_total = sum(totals_bytes.values()) or 0

    rows = []
    for raw_service, size_bytes in totals_bytes.items():
        service_name, code = split_service_and_code(raw_service)
        if SKIP_EMPTY_SERVICE and not service_name:
            continue

        percent = 0.0
        if grand_total > 0:
            percent = (size_bytes / grand_total) * 100.0

        rows.append(
            {
                "service_name": service_name,
                "code": code,
                "size_human": format_size(size_bytes, binary=True),
                "percent_consumption": round(percent, 4),
                "size_bytes": size_bytes,
            }
        )

    rows.sort(key=lambda x: x["size_bytes"], reverse=True)

    logging.info(f"hosted repos: {hosted_total}")
    logging.info(f"skipped without service: {skipped_no_service}")
    logging.info(f"services in report: {len(rows)}")
    logging.info(f"write excel: {out_file}")

    write_excel(out_file, rows)

    logging.info("done")


if __name__ == "__main__":
    main()
