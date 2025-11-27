import os
import logging
import requests
import xlsxwriter
import urllib3
import sqlite3
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

DB_PATH = "sonar_history.db"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ce_tasks_history (
        id TEXT PRIMARY KEY,
        project_key TEXT,
        status TEXT,
        type TEXT,
        executedAt TEXT,
        createdAt TEXT,
        updatedAt TEXT
    )
    """)
    conn.commit()
    conn.close()


def save_ce_tasks_to_db(project_key, tasks):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    new_count = 0
    for t in tasks:
        try:
            cur.execute("""
                INSERT INTO ce_tasks_history (id, project_key, status, type, executedAt, createdAt, updatedAt)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                t.get("id"),
                project_key,
                t.get("status"),
                t.get("type"),
                t.get("executionTime"),
                t.get("submittedAt"),
                t.get("updatedAt")
            ))
            new_count += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return new_count


def get_total_runs_from_db(project_key):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ce_tasks_history WHERE project_key = ?", (project_key,))
    total = cur.fetchone()[0]
    conn.close()
    return total


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
    params = {"componentKeys": project_key, "ps": 1}
    r = session.get(url, params=params, verify=False)
    r.raise_for_status()
    return r.json().get("total", 0)


def get_ce_tasks(project_key):
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
    headers_projects = [
        "project_name",
        "lines_of_code",
        "issues_total",
        "run_count_total"
    ]
    for col, h in enumerate(headers_projects):
        ws_projects.write(0, col, h)

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

        _, task_list = get_ce_tasks(key)

        added = save_ce_tasks_to_db(key, task_list)
        logger.info(f"Добавлено новых задач в БД: {added}")

        run_count_total = get_total_runs_from_db(key)
        total_runs += run_count_total

        ws_projects.write(row, 0, name)
        ws_projects.write(row, 1, ncloc)
        ws_projects.write(row, 2, issues_total)
        ws_projects.write(row, 3, run_count_total)

        for t in task_list:
            ws_tasks.write(tasks_row, 0, key)
            ws_tasks.write(tasks_row, 1, t.get("id"))
            tasks_row += 1

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


def main():
    init_db()
    logger.info("Получение пользователей")
    users = get_sonar_users()
    logger.info("Получение проектов")
    projects = get_projects()
    write_report(users, projects)


if __name__ == "__main__":
    main()
