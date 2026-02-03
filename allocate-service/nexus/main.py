import os
import logging
from dotenv import load_dotenv

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

from humanfriendly import format_size

from nexus_sizes import get_repository_data, get_repository_sizes
from confluence_names import confluence_table_as_dicts, repo_to_service_map


def write_excel(path, rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "report"

    header = ["service_name", "size_gib", "size_human"]
    ws.append(header)

    bold = Font(bold=True)
    for i in range(1, len(header) + 1):
        ws.cell(row=1, column=i).font = bold

    for r in rows:
        ws.append([r["service_name"], r["size_gib"], r["size_human"]])

    widths = [len(h) for h in header]
    for r in rows:
        widths[0] = max(widths[0], len(str(r["service_name"])))
        widths[1] = max(widths[1], len(str(r["size_gib"])))
        widths[2] = max(widths[2], len(str(r["size_human"])))

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
    out_file = os.getenv("OUT_FILE", "nexus_service_sizes.xlsx")

    if not conf_url or not conf_page_id or not conf_user or not conf_pass:
        raise RuntimeError("Нужны CONF_URL, CONF_PAGE_ID, CONF_USER, CONF_PASS")

    logging.info("Читаю таблицу из Confluence")
    conf_rows = confluence_table_as_dicts(conf_url, conf_page_id, conf_user, conf_pass)
    repo_service = repo_to_service_map(conf_rows)
    logging.info(f"mapped repos from confluence: {len(repo_service)}")

    logging.info("Читаю репозитории из БД")
    repo_data = get_repository_data()
    logging.info(f"repos from db: {len(repo_data)}")

    logging.info("Считаю размеры репозиториев из БД")
    repo_sizes = get_repository_sizes()
    logging.info(f"repos with size: {len(repo_sizes)}")

    hosted_total = 0
    matched_repos = 0
    totals = {}

    for r in repo_data:
        if (r.get("repository_type") or "").strip().lower() != "hosted":
            continue

        hosted_total += 1
        repo_name = r["repository_name"]

        service_name = repo_service.get(repo_name)
        if not service_name:
            continue

        matched_repos += 1
        size_bytes = repo_sizes.get(repo_name, 0) or 0
        totals[service_name] = totals.get(service_name, 0) + size_bytes

    rows = []
    for service_name, size_bytes in totals.items():
        rows.append(
            {
                "service_name": service_name,
                "size_gib": round(size_bytes / (1024 ** 3), 4),
                "size_human": format_size(size_bytes, binary=True),
            }
        )

    rows.sort(key=lambda x: x["service_name"])

    logging.info(f"hosted repos: {hoste
