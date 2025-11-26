import os
import logging
import requests
import xlsxwriter
import urllib3
from bs4 import BeautifulSoup
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


# ------------------------------------------------------------------------
# USERS
# ------------------------------------------------------------------------
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


# ------------------------------------------------------------------------
# PROJECTS
# ------------------------------------------------------------------------
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


# ------------------------------------------------------------------------
# Ncloc & Issues
# ------------------------------------------------------------------------
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
    params = {"componentKeys": project_key, "ps": 1}
    r = session.get(url, params=params, verify=False)
    r.raise_for_status()
    return r.json().get("total", 0)


# ------------------------------------------------------------------------
# NEW: CE ACTIVITY API (CORRECT RUN COUNT)
# ------------------------------------------------------------------------
def get_ce_tasks(project_key):
    """
    Возвращает:
    - count (общее число анализов)
    - tasks (список всех задач с ID)
    """
    page = 1
    page_size = 100
    all_tasks = []

    while True:
        url = f"{SONAR_URL}/api/ce/activity"
        params = {
            "status": "IN_PROGRESS,SUCCESS,FAILED,CANCELED",
            "component": project_key,
            "p": page,
            "ps": page_size
        }

        logger.info(f"CE ACTIVITY GET {url} params={params}")
        r = session.get(url, params=params, verify=False)
        r.raise_for_status()

        data = r.json()
        tasks = data.get("tasks", [])
        paging_total = data.get("paging", {}).get("total", len(tasks))

        all_tasks.extend(tasks)

        if page * page_size >= paging_total:
            break

        page += 1

    return len(all_tasks), all_tasks


# ------------------------------------------------------------------------
# REPORT
# ------------------------------------------------------------------------
def write_report(users, projects, filename="sonar_report.xlsx"):
    workbook = xlsxwriter.Workbook(filename)

    # USERS SHEET
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

    # PROJECTS SHEET
    ws_projects = workbook.add_worksheet("projects")
    headers_projects = [
        "project_name",
        "ncloc",
        "issues_total",
        "run_count"
    ]
    for col, h in enumerate(headers_projects):
        ws_projects.write(0, col, h)

    # TASKS SHEET — NEW
    ws_tasks = workbook.add_worksheet("tasks")
    ws_tasks.write(0, 0, "project_key")
    ws_tasks.write(0, 1, "task_id")

    tasks_row = 1
    total_runs = 0

    for row, p in enumerate(projects, start=1):
        key = p.get("key")
        name = p.get("name")

        logger.info(f"Проект: {key}")

        ncloc = get_ncloc(key)
        issues_total = get_issues_count(key)

        # NEW: run count from CE activity
        run_count, task_list = get_ce_tasks(key)
        total_runs += run_count

        # Пишем проект
        ws_projects.write(row, 0, name)
        ws_projects.write(row, 1, ncloc)
        ws_projects.write(row, 2, issues_total)
        ws_projects.write(row, 3, run_count)

        # Пишем все task.id
        for t in task_list:
            ws_tasks.write(tasks_row, 0, key)
            ws_tasks.write(tasks_row, 1, t.get("id"))
            tasks_row += 1

    # SUMMARY SHEET
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

    ws_summary.write(4, 0, "total_runs")
    ws_summary.write(4, 1, total_runs)

    workbook.close()
    logger.info("Excel сформирован")


# ------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------
def main():
    logger.info("Получение пользователей")
    users = get_sonar_users()

    logger.info("Получение проектов")
    projects = get_projects()

    write_report(users, projects)


if __name__ == "__main__":
    main()
