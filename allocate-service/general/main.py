import os
import logging
import requests
import urllib3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font

from reference_loader import load_reference_rows

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

CONF_URL = os.getenv("CONF_URL", "").strip()
CONF_PAGE_ID = os.getenv("CONF_PAGE_ID", "").strip()
CONF_USER = os.getenv("CONF_USER", "").strip()
CONF_PASS = os.getenv("CONF_PASS", "").strip()

REFERENCES_DIR = "references"
OUT_FILE = "employees.xlsx"


def fetch_confluence_table():
    if not CONF_URL or not CONF_PAGE_ID or not CONF_USER or not CONF_PASS:
        raise RuntimeError("CONF_URL / CONF_PAGE_ID / CONF_USER / CONF_PASS are not set")

    url = f"{CONF_URL.rstrip('/')}/rest/api/content/{CONF_PAGE_ID}"
    params = {"expand": "body.storage"}

    log.info("Requesting Confluence page %s", CONF_PAGE_ID)
    r = requests.get(
        url,
        params=params,
        auth=(CONF_USER, CONF_PASS),
        timeout=30,
        verify=False,
    )
    r.raise_for_status()

    html = r.json()["body"]["storage"]["value"]

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("Table not found on Confluence page")

    rows = table.find_all("tr")
    if not rows:
        raise RuntimeError("No rows found in Confluence table")

    employees = []

    for tr in rows[1:]:
        cols = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
        if not cols:
            continue

        employee = cols[0].strip() if len(cols) > 0 else ""
        load_value = cols[1].strip() if len(cols) > 1 else ""

        if not employee:
            continue

        employees.append(
            {
                "employee": employee,
                "load": load_value,
            }
        )

    log.info("Loaded %s employees from Confluence", len(employees))
    return employees


def bold_row(ws, row_num: int):
    bold_font = Font(bold=True)
    for cell in ws[row_num]:
        cell.font = bold_font


def build_source_map(rows, source_name: str):
    result = {}

    for row in rows:
        service_name = str(row.get("service_name", "") or "").strip()
        service_code = str(row.get("service_code", "") or "").strip()
        percent = row.get("percent")

        if not service_name and not service_code:
            continue

        key = (service_name, service_code)

        if key not in result:
            result[key] = {
                "service_name": service_name,
                "service_code": service_code,
                source_name: percent,
            }
        else:
            current = result[key].get(source_name)
            if current is None:
                result[key][source_name] = percent
            elif percent is not None:
                result[key][source_name] = current + percent

    return result


def merge_source_maps(source_maps):
    merged = {}

    for source_map in source_maps:
        for key, item in source_map.items():
            if key not in merged:
                merged[key] = dict(item)
                continue

            for k, v in item.items():
                if k in ("service_name", "service_code"):
                    continue
                merged[key][k] = v

    result = list(merged.values())
    result.sort(key=lambda x: (x["service_name"].lower(), x["service_code"]))
    return result


def write_employees_block(ws, employees, start_row=1, start_col=1):
    row = start_row
    col = start_col

    ws.cell(row=row, column=col, value="ФИО")
    ws.cell(row=row, column=col + 1, value="Загрузка")
    bold_row(ws, row)

    row += 1
    for item in employees:
        ws.cell(row=row, column=col, value=item["employee"])
        ws.cell(row=row, column=col + 1, value=item["load"])
        row += 1

    return row


def write_services_matrix(ws, merged_rows, source_columns, start_row, start_col=1):
    top_header_row = start_row
    description_row = start_row + 1
    table_header_row = start_row + 2
    data_start_row = start_row + 3

    ws.cell(row=table_header_row, column=start_col, value="Service name")
    ws.cell(row=table_header_row, column=start_col + 1, value="Code")

    current_col = start_col + 2
    for source_name in source_columns:
        ws.cell(row=top_header_row, column=current_col, value=source_name)
        ws.cell(row=description_row, column=current_col, value="")
        ws.cell(row=table_header_row, column=current_col, value="%")
        current_col += 1

    bold_font = Font(bold=True)
    for col in range(start_col, current_col):
        ws.cell(row=top_header_row, column=col).font = bold_font
        ws.cell(row=table_header_row, column=col).font = bold_font

    row = data_start_row
    for item in merged_rows:
        ws.cell(row=row, column=start_col, value=item["service_name"])
        ws.cell(row=row, column=start_col + 1, value=item["service_code"])

        current_col = start_col + 2
        for source_name in source_columns:
            value = item.get(source_name)
            ws.cell(row=row, column=current_col, value=value)
            current_col += 1

        row += 1

    return row


def export_report(employees, merged_rows, source_columns, out_file):
    wb = Workbook()
    ws = wb.active
    ws.title = "report"

    log.info("Writing employees block")
    next_row = write_employees_block(ws, employees, start_row=1, start_col=1)

    next_row += 2

    log.info("Writing services matrix")
    write_services_matrix(
        ws=ws,
        merged_rows=merged_rows,
        source_columns=source_columns,
        start_row=next_row,
        start_col=1,
    )

    wb.save(out_file)
    log.info("Report saved to %s", out_file)


def main():
    log.info("Started report generation")

    employees = fetch_confluence_table()

    log.info("Loading Nexus reference")
    nexus_rows = load_reference_rows(
        file_path=os.path.join(REFERENCES_DIR, "nexus.xlsx"),
        service_name_col=2,
        service_code_col=3,
        owner_col=4,
        percent_col=6,
        header_row=1,
    )

    nexus_map = build_source_map(nexus_rows, "Nexus")
    merged_rows = merge_source_maps([nexus_map])

    export_report(
        employees=employees,
        merged_rows=merged_rows,
        source_columns=["Nexus"],
        out_file=OUT_FILE,
    )

    log.info("Finished report generation")


if __name__ == "__main__":
    main()