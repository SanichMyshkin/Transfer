import os
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

CONF_URL = os.getenv("CONF_URL")
CONF_PAGE_ID = os.getenv("CONF_PAGE_ID")
CONF_USER = os.getenv("CONF_USER")
CONF_PASS = os.getenv("CONF_PASS")

REFERENCES_DIR = "references"
OUT_FILE = "employees.xlsx"


def fetch_confluence_table():
    url = f"{CONF_URL.rstrip('/')}/rest/api/content/{CONF_PAGE_ID}"
    params = {"expand": "body.storage"}

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
    headers = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]

    employee_header = headers[0]
    load_header = headers[1]
    total_header = headers[2]
    service_headers = headers[3:]

    data_rows = []

    for tr in rows[1:]:
        cols = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
        if not cols:
            continue

        employee = cols[0].strip()
        if not employee:
            continue

        load_value = cols[1] if len(cols) > 1 else ""
        services = cols[3:] if len(cols) > 3 else []

        if len(services) < len(service_headers):
            services += [""] * (len(service_headers) - len(services))

        data_rows.append(
            {
                "employee": employee,
                "load": load_value,
                "services": dict(zip(service_headers, services)),
            }
        )

    return employee_header, load_header, total_header, service_headers, data_rows


def bold_header(ws):
    font = Font(bold=True)
    for cell in ws[1]:
        cell.font = font


def build_nexus_map(nexus_rows):
    result = {}

    for row in nexus_rows:
        owner = row.get("owner", "").strip()
        service = row.get("service_display", "")
        percent = row.get("percent")

        if not owner:
            continue

        if owner not in result:
            result[owner] = {
                "services": [],
                "percent": 0,
            }

        result[owner]["services"].append(service)

        if percent:
            result[owner]["percent"] += percent

    return result


def export_report(
    employee_header,
    load_header,
    total_header,
    service_headers,
    data_rows,
    nexus_map,
):
    wb = Workbook()
    ws = wb.active
    ws.title = "employees"

    header = [
        employee_header,
        load_header,
        total_header,
        "Nexus services",
        "Nexus percent",
    ] + service_headers

    ws.append(header)
    bold_header(ws)

    for row in data_rows:
        employee = row["employee"]
        load_value = row["load"]

        nexus_services = ""
        nexus_percent = ""

        if employee in nexus_map:
            nexus_services = ", ".join(nexus_map[employee]["services"])
            nexus_percent = nexus_map[employee]["percent"] / 100

        ws_row = [
            employee,
            load_value,
            None,
            nexus_services,
            nexus_percent,
        ]

        for service in service_headers:
            ws_row.append(row["services"].get(service, ""))

        ws.append(ws_row)

    service_start = 6
    service_end = service_start + len(service_headers) - 1

    for i in range(2, ws.max_row + 1):
        start = get_column_letter(service_start)
        end = get_column_letter(service_end)
        ws.cell(row=i, column=3).value = f"=SUM({start}{i}:{end}{i})"

    for row in ws.iter_rows(min_row=2, min_col=5, max_col=5):
        for cell in row:
            if isinstance(cell.value, (int, float)):
                cell.number_format = "0.00%"

    wb.save(OUT_FILE)


def main():
    employee_header, load_header, total_header, service_headers, data_rows = fetch_confluence_table()

    nexus_rows = load_reference_rows(
        file_path=os.path.join(REFERENCES_DIR, "nexus.xlsx"),
        service_name_col=2,
        service_code_col=3,
        owner_col=4,
        percent_col=6,
    )

    nexus_map = build_nexus_map(nexus_rows)

    export_report(
        employee_header,
        load_header,
        total_header,
        service_headers,
        data_rows,
        nexus_map,
    )

    print(f"Report created: {OUT_FILE}")


if __name__ == "__main__":
    main()