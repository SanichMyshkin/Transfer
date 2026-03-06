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

CONF_URL = os.getenv("CONF_URL", "").strip()
CONF_PAGE_ID = os.getenv("CONF_PAGE_ID", "").strip()
CONF_USER = os.getenv("CONF_USER", "").strip()
CONF_PASS = os.getenv("CONF_PASS", "").strip()

REFERENCES_DIR = "references"
OUT_FILE = "employees.xlsx"


def set_bold_row(ws, row_num, col_count):
    font = Font(bold=True)
    for col in range(1, col_count + 1):
        ws.cell(row=row_num, column=col).font = font


def try_parse_percent(value):
    if value is None:
        return None

    s = str(value).strip().replace(",", ".")
    if not s:
        return None

    if "/" in s:
        return None

    if s.endswith("%"):
        s = s[:-1].strip()

    if not re.fullmatch(r"\d+(\.\d+)?", s):
        return None

    num = float(s)
    if num > 1:
        return num / 100
    return num


def fetch_confluence_table():
    if not CONF_URL or not CONF_PAGE_ID or not CONF_USER or not CONF_PASS:
        raise RuntimeError("CONF_URL / CONF_PAGE_ID / CONF_USER / CONF_PASS are not set")

    url = f"{CONF_URL.rstrip('/')}/rest/api/content/{CONF_PAGE_ID}"
    params = {"expand": "body.storage"}

    log.info("Requesting Confluence page %s", CONF_PAGE_ID)
    response = requests.get(
        url,
        params=params,
        auth=(CONF_USER, CONF_PASS),
        timeout=30,
        verify=False,
    )
    response.raise_for_status()

    html = response.json()["body"]["storage"]["value"]

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("Table not found on Confluence page")

    rows = table.find_all("tr")
    if not rows:
        raise RuntimeError("No rows found in Confluence table")

    headers = [cell.get_text(strip=True) for cell in rows[0].find_all(["th", "td"])]
    if len(headers) < 4:
        raise RuntimeError("Expected at least 4 columns in Confluence table")

    service_headers = headers[3:]
    data_rows = []

    for tr in rows[1:]:
        cols = [cell.get_text(strip=True) for cell in tr.find_all(["th", "td"])]
        if not cols:
            continue

        employee = cols[0].strip() if len(cols) > 0 else ""
        if not employee:
            continue

        load_value = cols[1].strip() if len(cols) > 1 else ""
        service_values = cols[3:] if len(cols) > 3 else []

        if len(service_values) < len(service_headers):
            service_values += [""] * (len(service_headers) - len(service_values))
        else:
            service_values = service_values[: len(service_headers)]

        data_rows.append(
            {
                "employee": employee,
                "load": load_value,
                "services": dict(zip(service_headers, service_values)),
            }
        )

    log.info("Loaded %s employees from Confluence", len(data_rows))
    return headers, service_headers, data_rows


def build_source_map(rows, source_name):
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
                f"{source_name}_percent": percent,
            }
            continue

        current = result[key].get(f"{source_name}_percent")
        if current is None:
            result[key][f"{source_name}_percent"] = percent
        elif percent is not None:
            result[key][f"{source_name}_percent"] = current + percent

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

                if k not in merged[key]:
                    merged[key][k] = v
                    continue

                current = merged[key][k]
                if current is None:
                    merged[key][k] = v
                elif v is not None:
                    merged[key][k] = current + v

    result = list(merged.values())
    result.sort(key=lambda x: (x["service_name"].lower(), x["service_code"]))
    return result


def write_employees_sheet(ws, headers, service_headers, data_rows):
    ws.append(headers)
    set_bold_row(ws, 1, len(headers))

    service_start_col = 4
    service_end_col = service_start_col + len(service_headers) - 1
    service_column_map = {}

    for idx, service_name in enumerate(service_headers, start=service_start_col):
        service_column_map[service_name] = idx

    for row_idx, item in enumerate(data_rows, start=2):
        row = [item["employee"], item["load"], None]

        for service_name in service_headers:
            raw_value = item["services"].get(service_name, "")
            parsed = try_parse_percent(raw_value)
            row.append(parsed if parsed is not None else raw_value)

        ws.append(row)

        if service_headers:
            start_letter = get_column_letter(service_start_col)
            end_letter = get_column_letter(service_end_col)
            total_cell = ws.cell(row=row_idx, column=3)
            total_cell.value = f"=SUM({start_letter}{row_idx}:{end_letter}{row_idx})"
            total_cell.number_format = "0%"

    for row in ws.iter_rows(
        min_row=2,
        max_row=ws.max_row,
        min_col=service_start_col,
        max_col=service_end_col,
    ):
        for cell in row:
            if isinstance(cell.value, (int, float)):
                cell.number_format = "0%"

    sum_row = ws.max_row + 1
    ws.cell(row=sum_row, column=1, value="SUM")

    if service_headers:
        for col in range(service_start_col, service_end_col + 1):
            col_letter = get_column_letter(col)
            cell = ws.cell(row=sum_row, column=col)
            cell.value = f"=SUM({col_letter}2:{col_letter}{sum_row - 1})"
            cell.number_format = "0%"

        total_start_letter = get_column_letter(service_start_col)
        total_end_letter = get_column_letter(service_end_col)
        total_sum_cell = ws.cell(row=sum_row, column=3)
        total_sum_cell.value = f"=SUM({total_start_letter}{sum_row}:{total_end_letter}{sum_row})"
        total_sum_cell.number_format = "0%"

    set_bold_row(ws, sum_row, len(headers))

    return service_column_map, sum_row


def write_sources_sheet(ws, merged_rows, source_columns, service_column_map, employees_sum_row):
    ws.cell(row=1, column=1, value="Service name")
    ws.cell(row=1, column=2, value="Code")
    ws.cell(row=2, column=1, value="")
    ws.cell(row=2, column=2, value="")

    ws.cell(row=1, column=1).font = Font(bold=True)
    ws.cell(row=1, column=2).font = Font(bold=True)

    current_col = 3
    for source_name in source_columns:
        ws.merge_cells(
            start_row=1,
            start_column=current_col,
            end_row=1,
            end_column=current_col + 1,
        )
        head_cell = ws.cell(row=1, column=current_col, value=source_name)
        head_cell.font = Font(bold=True)
        head_cell.alignment = Alignment(horizontal="center")

        percent_head = ws.cell(row=2, column=current_col, value="%")
        weight_head = ws.cell(row=2, column=current_col + 1, value="weight")
        percent_head.font = Font(bold=True)
        weight_head.font = Font(bold=True)

        current_col += 2

    for row_idx, item in enumerate(merged_rows, start=3):
        service_name = item["service_name"]
        service_code = item["service_code"]

        ws.cell(row=row_idx, column=1, value=service_name)
        ws.cell(row=row_idx, column=2, value=service_code)

        employees_service_col = service_column_map.get(service_name)

        current_col = 3
        for source_name in source_columns:
            percent_value = item.get(f"{source_name}_percent")
            percent_cell = ws.cell(row=row_idx, column=current_col, value=percent_value)

            if isinstance(percent_value, (int, float)):
                percent_cell.number_format = "0.0000"

            weight_cell = ws.cell(row=row_idx, column=current_col + 1)

            if employees_service_col is not None:
                employees_col_letter = get_column_letter(employees_service_col)
                percent_col_letter = get_column_letter(current_col)
                weight_cell.value = (
                    f"=Employees!{employees_col_letter}{employees_sum_row}"
                    f"*{percent_col_letter}{row_idx}"
                )
                weight_cell.number_format = "0.0000"

            current_col += 2


def main():
    log.info("Started report generation")

    headers, service_headers, employees = fetch_confluence_table()

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

    wb = Workbook()

    ws_employees = wb.active
    ws_employees.title = "Employees"
    service_column_map, employees_sum_row = write_employees_sheet(
        ws_employees,
        headers,
        service_headers,
        employees,
    )

    ws_sources = wb.create_sheet("Sources")
    write_sources_sheet(
        ws_sources,
        merged_rows,
        ["Nexus"],
        service_column_map,
        employees_sum_row,
    )

    wb.save(OUT_FILE)

    log.info("Report saved to %s", OUT_FILE)
    log.info("Finished report generation")


if __name__ == "__main__":
    main()