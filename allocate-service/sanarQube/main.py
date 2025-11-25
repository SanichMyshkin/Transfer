import os
import logging
import requests
import xlsxwriter
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

SONAR_URL = os.getenv("SONAR_URL")
TOKEN = os.getenv("SONAR_TOKEN")

if not SONAR_URL or not TOKEN:
    logger.error("Не заданы переменные окружения SONAR_URL и SONAR_TOKEN")
    raise SystemExit(1)

session = requests.Session()
session.auth = (TOKEN, "")


def get_sonar_users():
    users = []
    page = 1
    page_size = 500

    while True:
        url = f"{SONAR_URL}/api/users/search"
        params = {"p": page, "ps": page_size}
        logger.info(f"GET {url} params={params}")
        r = session.get(url, params=params, verify=False)
        r.raise_for_status()
        data = r.json()
        page_users = data.get("users", [])
        total = data.get("paging", {}).get("total", 0)
        users.extend(page_users)
        if page * page_size >= total:
            break
        page += 1

    return users


def get_projects():
    projects = []
    page = 1
    size = 500

    while True:
        url = f"{SONAR_URL}/api/projects/search"
        params = {"p": page, "ps": size}
        logger.info(f"GET {url} params={params}")
        r = session.get(url, params=params, verify=False)
        r.raise_for_status()
        data = r.json()
        parts = data.get("components", [])
        total = data.get("paging", {}).get("total", 0)
        projects.extend(parts)
        if page * size >= total:
            break
        page += 1

    return projects


def get_ncloc(project_key):
    url = f"{SONAR_URL}/api/measures/component"
    params = {"component": project_key, "metricKeys": "ncloc"}
    r = session.get(url, params=params, verify=False)
    r.raise_for_status()
    measures = r.json().get("component", {}).get("measures", [])
    if not measures:
        return 0
    return int(measures[0].get("value", 0))


def get_issues_count(project_key):
    url = f"{SONAR_URL}/api/issues/search"
    params = {"projectKeys": project_key, "ps": 1}
    r = session.get(url, params=params, verify=False)
    r.raise_for_status()
    return r.json().get("total", 0)


def get_analysis_count(project_key):
    url = f"{SONAR_URL}/api/project_analyses/search"
    params = {"projects": project_key}
    r = session.get(url, params=params, verify=False)
    r.raise_for_status()
    return len(r.json().get("analyses", []))


def write_report(users, projects, filename="sonar_report.xlsx"):
    workbook = xlsxwriter.Workbook(filename)

    ws_users = workbook.add_worksheet("users")
    headers_users = [
        "login", "name", "email", "active", "groups", "tokensCount",
        "local", "externalIdentity", "externalProvider", "avatar",
        "lastConnectionDate", "managed"
    ]
    for col, h in enumerate(headers_users):
        ws_users.write(0, col, h)

    for row, u in enumerate(users, start=1):
        ws_users.write(row, 0, u.get("login"))
        ws_users.write(row, 1, u.get("name") or u.get("fullName"))
        ws_users.write(row, 2, u.get("email"))
        ws_users.write(row, 3, u.get("active"))
        ws_users.write(row, 4, "; ".join(u.get("groups", [])))
        ws_users.write(row, 5, u.get("tokensCount"))
        ws_users.write(row, 6, u.get("local"))
        ws_users.write(row, 7, u.get("externalIdentity"))
        ws_users.write(row, 8, u.get("externalProvider"))
        ws_users.write(row, 9, u.get("avatar"))
        ws_users.write(row, 10, u.get("lastConnectionDate"))
        ws_users.write(row, 11, u.get("managed"))

    ws_projects = workbook.add_worksheet("projects")
    headers_projects = ["project_name", "ncloc", "issues_total", "analyses_count"]
    for col, h in enumerate(headers_projects):
        ws_projects.write(0, col, h)

    total_analyses = 0

    for row, p in enumerate(projects, start=1):
        key = p.get("key")
        name = p.get("name")

        logger.info(f"Проект: {key}")

        ncloc = get_ncloc(key)
        issues_total = get_issues_count(key)
        analyses_count = get_analysis_count(key)
        total_analyses += analyses_count

        ws_projects.write(row, 0, name)
        ws_projects.write(row, 1, ncloc)
        ws_projects.write(row, 2, issues_total)
        ws_projects.write(row, 3, analyses_count)

    ws_summary = workbook.add_worksheet("summary")
    local_users = len([u for u in users if u.get("local")])
    external_users = len([u for u in users if not u.get("local")])

    ws_summary.write(0, 0, "total_users")
    ws_summary.write(0, 1, len(users))

    ws_summary.write(1, 0, "local_users")
    ws_summary.write(1, 1, local_users)

    ws_summary.write(2, 0, "external_users")
    ws_summary.write(2, 1, external_users)

    ws_summary.write(3, 0, "total_projects")
    ws_summary.write(3, 1, len(projects))

    ws_summary.write(4, 0, "total_analyses")
    ws_summary.write(4, 1, total_analyses)

    workbook.close()
    logger.info("Excel сформирован")


def main():
    logger.info("Получение пользователей")
    users = get_sonar_users()

    logger.info("Получение проектов")
    projects = get_projects()

    write_report(users, projects)


if __name__ == "__main__":
    main()
