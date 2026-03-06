import os
import re
import logging
import requests
import urllib3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font
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

    headers = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
    service_headers = headers[3:]

    data_rows = []

    for tr in rows[1:]:
        cols = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
        if not cols:
            continue

        employee = cols[0].strip()
        if not employee:
            continue

        load_value = cols[1].strip() if len(cols) > 1 else ""
        service_values = cols[3:] if len(cols) > 3 else []

        if len(service_values) < len(service_headers):
            service_values += [""] * (len(service_headers) - len(service_values))
        else:
            service_values = service_values[:len(service_headers)]

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
        owner = str(row.get("owner", "") or "").strip()
        percent = row.get("percent")

        if not service_name and not service_code:
            continue

        key = (service_name, service_code)

        if key not in result:
            result[key] = {
                "service_name": service_name,
                "service_code": service_code,
                f"{source_name}_owner": owner,
                f"{source_name}_percent": percent,
            }
        else:
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
                merged[key][k] = v

    result = list(merged.values())
    result.sort(key=lambda x: (x["service_name"].lower(), x["service_code"]))
    return result


def write_employees_sheet(ws, headers, service_headers, data_rows):
    ws.append(headers)
    set_bold_row(ws, 1, len(headers))

    service_start_col = 4
    service_end_col = service_start_col + len(service_headers) - 1
    employee_row_map = {}

    for row_idx, item in enumerate(data_rows, start=2):
        row = [item["employee"], item["load"], None]

        for service_name in service_headers:
            raw_value = item["services"].get(service_name, "")
            parsed = try_parse_percent(raw_value)
            row.append(parsed if parsed is not None else raw_value)

        ws.append(row)
        employee_row_map[item["employee"]] = row_idx

        if service_headers:
            start_letter = get_column_letter(service_start_col)
            end_letter = get_column_letter(service_end_col)
            total_cell = ws.cell(row=row_idx, column=3)
            total_cell.value = f"=SUM({start_letter}{row_idx}:{end_letter}{row_idx})"
            total_cell.number_format = "0%"

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=4, max_col=3 + len(service_headers)):
        for cell in row:
            if isinstance(cell.value, (int, float)):
                cell.number_format = "0%"

    return employee_row_map


def write_sources_sheet(ws, merged_rows, source_columns, employee_row_map):
    headers = ["Service name", "Code"]

    for source_name in source_columns:
        headers.append(f"{source_name} %")
        headers.append(f"{source_name} weighted")

    ws.append(headers)
    set_bold_row(ws, 1, len(headers))

    for row_idx, item in enumerate(merged_rows, start=2):
        row = [item["service_name"], item["service_code"]]
        ws.append(row)

        current_col = 3
        for source_name in source_columns:
            percent_value = item.get(f"{source_name}_percent")
            owner = str(item.get(f"{source_name}_owner", "") or "").strip()

            percent_cell = ws.cell(row=row_idx, column=current_col, value=percent_value)
            if isinstance(percent_value, (int, float)):
                percent_cell.number_format = "0.0000"

            weighted_cell = ws.cell(row=row_idx, column=current_col + 1)

            owner_row = employee_row_map.get(owner)
            if owner_row:
                percent_col_letter = get_column_letter(current_col)
                weighted_cell.value = f"={percent_col_letter}{row_idx}*Employees!C{owner_row}"
                weighted_cell.number_format = "0.0000"
            else:
                weighted_cell.value = None

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
    employee_row_map = write_employees_sheet(ws_employees, headers, service_headers, employees)

    ws_sources = wb.create_sheet("Sources")
    write_sources_sheet(ws_sources, merged_rows, ["Nexus"], employee_row_map)

    wb.save(OUT_FILE)

    log.info("Report saved to %s", OUT_FILE)
    log.info("Finished report generation")


if __name__ == "__main__":
    main()