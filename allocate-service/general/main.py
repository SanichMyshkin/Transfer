import os
import re
import logging

import requests
import urllib3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter

from reference_loader import load_reference_rows

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger(__name__)

CONF_URL = os.getenv("CONF_URL")
CONF_PAGE_ID = os.getenv("CONF_PAGE_ID")
CONF_USER = os.getenv("CONF_USER")
CONF_PASS = os.getenv("CONF_PASS")

REFERENCES_DIR = "references"
OUT_FILE = "employees.xlsx"


def set_bold_row(ws, row, cols):
    for col in range(1, cols + 1):
        ws.cell(row=row, column=col).font = Font(bold=True)


def try_parse_percent(value):

    if value is None:
        return None

    s = str(value).strip().replace(",", ".")

    if "/" in s:
        return None

    if s.endswith("%"):
        s = s[:-1]

    if not s:
        return None

    if not re.fullmatch(r"\d+(\.\d+)?", s):
        return None

    num = float(s)

    if num > 1:
        return num / 100

    return num


def fetch_confluence_table():

    url = f"{CONF_URL.rstrip('/')}/rest/api/content/{CONF_PAGE_ID}"
    params = {"expand": "body.storage"}

    log.info("Loading Confluence table")

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

    rows = table.find_all("tr")

    headers = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
    service_headers = headers[3:]

    data_rows = []

    for tr in rows[1:]:

        cols = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]

        if not cols:
            continue

        employee = cols[0]
        load_value = cols[1] if len(cols) > 1 else ""

        service_values = cols[3:]

        if len(service_values) < len(service_headers):
            service_values += [""] * (len(service_headers) - len(service_values))

        data_rows.append(
            {
                "employee": employee,
                "load": load_value,
                "services": dict(zip(service_headers, service_values)),
            }
        )

    return headers, service_headers, data_rows


def write_employees_sheet(ws, headers, service_headers, data_rows):

    ws.append(headers)
    set_bold_row(ws, 1, len(headers))

    platform_col_map = {}

    service_start = 4
    service_end = service_start + len(service_headers) - 1

    for idx, service in enumerate(service_headers, start=service_start):
        platform_col_map[service] = idx

    for row_idx, item in enumerate(data_rows, start=2):

        row = [item["employee"], item["load"], None]

        for service in service_headers:
            raw = item["services"].get(service, "")
            parsed = try_parse_percent(raw)
            row.append(parsed if parsed is not None else raw)

        ws.append(row)

        start_letter = get_column_letter(service_start)
        end_letter = get_column_letter(service_end)

        total_cell = ws.cell(row=row_idx, column=3)
        total_cell.value = f"=SUM({start_letter}{row_idx}:{end_letter}{row_idx})"
        total_cell.number_format = "0%"

    sum_row = ws.max_row + 1

    ws.cell(row=sum_row, column=1, value="SUM")

    for col in range(service_start, service_end + 1):

        col_letter = get_column_letter(col)

        cell = ws.cell(row=sum_row, column=col)
        cell.value = f"=SUM({col_letter}2:{col_letter}{sum_row - 1})"
        cell.number_format = "0%"

    set_bold_row(ws, sum_row, len(headers))

    return platform_col_map, sum_row


def build_source_rows(rows, source_name):

    result = []

    for r in rows:

        service_name = str(r.get("service_name", "")).strip()
        service_code = str(r.get("service_code", "")).strip()
        percent = r.get("percent")

        if not service_name:
            continue

        result.append(
            {
                "service_name": service_name,
                "service_code": service_code,
                f"{source_name}_percent": percent,
            }
        )

    return result


def merge_source_rows(rows_list):

    merged = {}

    for rows in rows_list:

        for item in rows:

            key = (item["service_name"], item["service_code"])

            if key not in merged:
                merged[key] = {
                    "service_name": item["service_name"],
                    "service_code": item["service_code"],
                }

            for k, v in item.items():
                if k in ("service_name", "service_code"):
                    continue

                merged[key][k] = v

    return list(merged.values())


def write_sources_sheet(ws, merged_rows, source_configs, platform_map, sum_row):

    ws.merge_cells(start_row=1, start_column=1, end_row=2, end_column=1)
    ws.merge_cells(start_row=1, start_column=2, end_row=2, end_column=2)

    ws.cell(row=1, column=1, value="Service name").font = Font(bold=True)
    ws.cell(row=1, column=2, value="Code").font = Font(bold=True)

    current_col = 3

    for cfg in source_configs:

        source = cfg["name"]
        subtitle = cfg["subtitle"]

        ws.merge_cells(
            start_row=1,
            start_column=current_col,
            end_row=1,
            end_column=current_col + 1,
        )

        head = ws.cell(row=1, column=current_col, value=source)
        head.font = Font(bold=True)
        head.alignment = Alignment(horizontal="center")

        ws.merge_cells(
            start_row=2,
            start_column=current_col,
            end_row=2,
            end_column=current_col + 1,
        )

        desc = ws.cell(row=2, column=current_col, value=subtitle)
        desc.alignment = Alignment(horizontal="center")

        current_col += 2

    for row_idx, item in enumerate(merged_rows, start=3):

        ws.cell(row=row_idx, column=1, value=item["service_name"])
        ws.cell(row=row_idx, column=2, value=item["service_code"])

        current_col = 3

        for cfg in source_configs:

            source = cfg["name"]

            percent_value = item.get(f"{source}_percent")

            percent_cell = ws.cell(row=row_idx, column=current_col, value=percent_value)

            if isinstance(percent_value, (int, float)):
                percent_cell.number_format = "0.0000"

            weight_cell = ws.cell(row=row_idx, column=current_col + 1)

            platform_col = platform_map.get(source)

            if platform_col:

                platform_letter = get_column_letter(platform_col)
                percent_letter = get_column_letter(current_col)

                weight_cell.value = (
                    f"=Employees!{platform_letter}{sum_row}"
                    f"*{percent_letter}{row_idx}"
                )

                weight_cell.number_format = "0.0000"

            current_col += 2


def main():

    headers, service_headers, employees = fetch_confluence_table()

    log.info("Loading Nexus data")

    nexus_rows = load_reference_rows(
        file_path=os.path.join(REFERENCES_DIR, "nexus.xlsx"),
        service_name_col=2,
        service_code_col=3,
        owner_col=4,
        percent_col=6,
        header_row=1,
    )

    log.info("Loading Gitlab data")

    gitlab_rows = load_reference_rows(
        file_path=os.path.join(REFERENCES_DIR, "gitlab.xlsx"),
        service_name_col=2,
        service_code_col=3,
        owner_col=4,
        percent_col=11,
        header_row=1,
    )

    source_configs = [
        {
            "name": "Nexus",
            "subtitle": "Объем репозиториев",
            "rows": build_source_rows(nexus_rows, "Nexus"),
        },
        {
            "name": "Gitlab",
            "subtitle": "Объем проектов",
            "rows": build_source_rows(gitlab_rows, "Gitlab"),
        },
    ]

    merged_rows = merge_source_rows([c["rows"] for c in source_configs])

    wb = Workbook()

    ws_emp = wb.active
    ws_emp.title = "Employees"

    platform_map, sum_row = write_employees_sheet(
        ws_emp,
        headers,
        service_headers,
        employees,
    )

    ws_sources = wb.create_sheet("Sources")

    write_sources_sheet(
        ws_sources,
        merged_rows,
        source_configs,
        platform_map,
        sum_row,
    )

    wb.save(OUT_FILE)

    log.info("Report saved to %s", OUT_FILE)


if __name__ == "__main__":
    main()