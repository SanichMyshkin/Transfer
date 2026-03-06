import os
import requests
import urllib3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openpyxl import Workbook

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

CONF_URL = os.getenv("CONF_URL")
CONF_PAGE_ID = os.getenv("CONF_PAGE_ID")
CONF_USER = os.getenv("CONF_USER")
CONF_PASS = os.getenv("CONF_PASS")


def confluence_table_dicts(conf_url, page_id, user, password):
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
    rows = table.find_all("tr")

    headers = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]

    # 0 = ФИО, 1 = загрузка, 2 = лишний столбец, с 3 начинаются сервисы
    service_headers = headers[3:]

    services_map = {}
    load_map = {}

    for tr in rows[1:]:
        cols = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]

        if not cols:
            continue

        employee = cols[0].strip()
        if not employee:
            continue

        load = cols[1].strip() if len(cols) > 1 else ""
        load_map[employee] = load

        services = {}
        for service, value in zip(service_headers, cols[3:]):
            value = value.strip()
            if value:
                services[service] = value

        services_map[employee] = services

    return services_map, load_map, service_headers


def export_to_excel(services_map, load_map, service_headers, filename="employees.xlsx"):
    wb = Workbook()

    # Лист с сервисами
    ws_services = wb.active
    ws_services.title = "services"

    ws_services.append(["ФИО"] + service_headers)
    for employee, services in services_map.items():
        row = [employee] + [services.get(service, "") for service in service_headers]
        ws_services.append(row)

    # Лист с загрузкой
    ws_load = wb.create_sheet("load")
    ws_load.append(["ФИО", "Загрузка"])
    for employee, load in load_map.items():
        ws_load.append([employee, load])

    wb.save(filename)


def main():
    services_map, load_map, service_headers = confluence_table_dicts(
        CONF_URL,
        CONF_PAGE_ID,
        CONF_USER,
        CONF_PASS,
    )

    export_to_excel(services_map, load_map, service_headers, "employees.xlsx")
    print("Файл employees.xlsx создан")


if __name__ == "__main__":
    main()