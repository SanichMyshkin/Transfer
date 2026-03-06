import os
import re
import requests
import urllib3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

CONF_URL = os.getenv("CONF_URL")
CONF_PAGE_ID = os.getenv("CONF_PAGE_ID")
CONF_USER = os.getenv("CONF_USER")
CONF_PASS = os.getenv("CONF_PASS")


def try_parse_percent(value: str):
    if value is None:
        return None

    s = str(value).strip().replace(",", ".")
    if not s:
        return None

    if "/" in s:
        return None

    if s.endswith("%"):
        num = s[:-1].strip()
        if re.fullmatch(r"\d+(\.\d+)?", num):
            return float(num) / 100

    if re.fullmatch(r"\d+(\.\d+)?", s):
        num = float(s)
        if num > 1:
            return num / 100
        return num

    return None


def confluence_table_data(conf_url, page_id, user, password):
    url = f"{conf_url.rstrip('/')}/rest/api/content/{page_id}"
    params = {"expand": "body.storage"}

    r = requests.get(
        url,
        params=params,
        auth=(user, password),
        timeout=30,
        verify=False,
    )
    r.raise_for_status()

    html = r.json()["body"]["storage"]["value"]

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("Таблица на странице не найдена")

    rows = table.find_all("tr")
    if not rows:
        raise RuntimeError("В таблице нет строк")

    headers = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
    if len(headers) < 4:
        raise RuntimeError("Ожидалось минимум 4 колонки")

    employee_header = headers[0]
    load_header = headers[1]
    total_header = headers[2]
    service_headers = headers[3:]

    data_rows = []

    for tr in rows[1:]:
        cols = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
        if not cols:
            continue

        employee = cols[0].strip() if len(cols) > 0 else ""
        if not employee:
            continue

        load_value = cols[1].strip() if len(cols) > 1 else ""
        service_values_raw = cols[3:] if len(cols) > 3 else []

        if len(service_values_raw) < len(service_headers):
            service_values_raw += [""] * (len(service_headers) - len(service_values_raw))
        else:
            service_values_raw = service_values_raw[:len(service_headers)]

        data_rows.append(
            {
                "employee": employee,
                "load": load_value,
                "services": dict(zip(service_headers, service_values_raw)),
            }
        )

    return employee_header, load_header, total_header, service_headers, data_rows


def export_to_excel(
    employee_header,
    load_header,
    total_header,
    service_headers,
    data_rows,
    filename="employees.xlsx",
):
    wb = Workbook()
    ws = wb.active
    ws.title = "employees"

    ws.append([employee_header, load_header, total_header] + service_headers)

    service_start_col = 4
    service_end_col = 3 + len(service_headers)

    for row_idx, item in enumerate(data_rows, start=2):
        employee = item["employee"]
        load_value = item["load"]
        services = item["services"]

        row = [employee, load_value, None]

        for service_name in service_headers:
            raw_value = services.get(service_name, "")
            parsed = try_parse_percent(raw_value)
            row.append(parsed if parsed is not None else raw_value)

        ws.append(row)

        start_letter = get_column_letter(service_start_col)
        end_letter = get_column_letter(service_end_col)
        ws.cell(row=row_idx, column=3).value = f"=SUM({start_letter}{row_idx}:{end_letter}{row_idx})"

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=3, max_col=service_end_col):
        for cell in row:
            if isinstance(cell.value, (int, float)):
                cell.number_format = "0%"

    wb.save(filename)


def main():
    employee_header, load_header, total_header, service_headers, data_rows = confluence_table_data(
        CONF_URL,
        CONF_PAGE_ID,
        CONF_USER,
        CONF_PASS,
    )

    export_to_excel(
        employee_header=employee_header,
        load_header=load_header,
        total_header=total_header,
        service_headers=service_headers,
        data_rows=data_rows,
        filename="employees.xlsx",
    )

    print("Файл employees.xlsx создан")


if __name__ == "__main__":
    main()