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
    logger.info("Инициализация базы данных...")
    with sqlite3.connect(DB_PATH) as conn:
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
    logger.info("База данных готова.")


def save_ce_tasks_to_db(project_key, tasks):
    new_count = 0
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
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
    logger.info(f"[DB] Новых задач добавлено: {new_count}")
    return new_count


def get_total_runs_from_db(project_key):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ce_tasks_history WHERE project_key = ?", (project_key,))
        total = cur.fetchone()[0]
    logger.info(f"[DB] Полное количество запусков проекта {project_key}: {total}")
    return total


def get_sonar_users():
    logger.info("Получение списка пользователей...")
    users = []
    page = 1
    size = 500
    while True:
        logger.info(f"[USERS] GET page={page}")
        r = session.get(f"{SONAR_URL}/api/users/search", params={"p": page, "ps": size}, verify=False)
        r.raise_for_status()
        data = r.json()
        batch = data.get("users", [])
        users.extend(batch)
        logger.info(f"[USERS] Получено {len(batch)} пользователей (всего: {len(users)})")
        if page * size >= data.get("paging", {}).get("total", 0):
            break
        page += 1
    logger.info(f"Пользователи получены: {len(users)}")
    return users


def get_projects():
    logger.info("Получение проектов...")
    projects = []
    page = 1
    size = 500
    while True:
        logger.info(f"[PROJECTS] GET page={page}")
        r = session.get(f"{SONAR_URL}/api/projects/search", params={"p": page, "ps": size}, verify=False)
        r.raise_for_status()
        data = r.json()
        batch = data.get("components", [])
        projects.extend(batch)
        logger.info(f"[PROJECTS] Получено {len(batch)} (всего: {len(projects)})")
        if page * size >= data.get("paging", {}).get("total", 0):
            break
        page += 1
    logger.info(f"Проекты получены: {len(projects)}")
    return projects


def get_ncloc(project_key):
    logger.info(f"[NCLOC] Получение строк кода для {project_key}")
    r = session.get(
        f"{SONAR_URL}/api/measures/component",
        params={"component": project_key, "metricKeys": "ncloc"},
        verify=False
    )
    r.raise_for_status()
    measures = r.json().get("component", {}).get("measures", [])
    n = int(measures[0].get("value", 0)) if measures else 0
    logger.info(f"[NCLOC] {project_key}: {n}")
    return n


def get_issues_count(project_key):
    r = session.get(
        f"{SONAR_URL}/api/issues/search",
        params={"componentKeys": project_key, "ps": 1},
        verify=False
    )
    r.raise_for_status()
    total = r.json().get("total", 0)
    logger.info(f"[ISSUES] {project_key}: {total}")
    return total


def get_ce_tasks(project_key):
    logger.info(f"[TASKS] Получение CE задач для {project_key}")
    page = 1
    size = 100
    tasks_all = []
    while True:
        logger.info(f"[TASKS] GET page={page}")
        r = session.get(
            f"{SONAR_URL}/api/ce/activity",
            params={
                "status": "IN_PROGRESS,SUCCESS,FAILED,CANCELED",
                "component": project_key,
                "p": page,
                "ps": size,
            },
            verify=False
        )
        r.raise_for_status()
        data = r.json()
        batch = data.get("tasks", [])
        tasks_all.extend(batch)
        logger.info(f"[TASKS] Получено {len(batch)} задач (всего: {len(tasks_all)})")
        if page * size >= data.get("paging", {}).get("total", 0):
            break
        page += 1
    return len(tasks_all), tasks_all


def write_report(users, projects, filename="sonar_report.xlsx"):
    logger.info("Формирование Excel отчёта...")

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

    row_tasks = 1
    total_runs = 0

    for i, p in enumerate(projects, start=1):
        key = p.get("key")
        name = p.get("name")

        logger.info(f"=== [{i}/{len(projects)}] Обработка проекта: {key} ===")

        ncloc = get_ncloc(key)
        issues = get_issues_count(key)

        _, tasks = get_ce_tasks(key)
        save_ce_tasks_to_db(key, tasks)

        runs_total = get_total_runs_from_db(key)
        total_runs += runs_total

        ws_projects.write(i, 0, name)
        ws_projects.write(i, 1, ncloc)
        ws_projects.write(i, 2, issues)
        ws_projects.write(i, 3, runs_total)

        for t in tasks:
            ws_tasks.write(row_tasks, 0, key)
            ws_tasks.write(row_tasks, 1, t.get("id"))
            row_tasks += 1

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
    logger.info(f"Отчёт сформирован: {filename}")


def main():
    logger.info("==== START ====")
    init_db()
    users = get_sonar_users()
    projects = get_projects()
    write_report(users, projects)
    logger.info("==== DONE ====")


if __name__ == "__main__":
    main()
