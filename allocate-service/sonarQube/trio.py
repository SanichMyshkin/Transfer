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


def create_instance(name, url, token):
    session = requests.Session()
    session.auth = (token, "")
    return {
        "name": name,
        "url": url.rstrip("/"),
        "session": session,
        "db_path": f"sonar_history_{name}.db",
    }


def load_instances():
    instances = []

    prod_url = os.getenv("PROD_URL")
    prod_token = os.getenv("PROD_TOKEN")
    if prod_url and prod_token:
        instances.append(create_instance("prod", prod_url, prod_token))

    fix_url = os.getenv("FIX_URL")
    fix_token = os.getenv("FIX_TOKEN")
    if fix_url and fix_token:
        instances.append(create_instance("fix", fix_url, fix_token))

    old_url = os.getenv("OLD_URL")
    old_token = os.getenv("OLD_TOKEN")
    if old_url and old_token:
        instances.append(create_instance("old", old_url, old_token))

    if not instances:
        logger.error("Нет ни одного конфигурированного инстанса (prod/fix/old)")
        raise SystemExit(1)

    for inst in instances:
        logger.info(f"[INIT] Инстанс загружен: {inst['name']} → {inst['url']} (DB: {inst['db_path']})")

    return instances


def init_db(db_path):
    logger.info(f"[DB INIT] Инициализация базы {db_path} ...")
    with sqlite3.connect(db_path) as conn:
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
    logger.info(f"[DB INIT] База {db_path} готова")


def save_ce_tasks_to_db(db_path, project_key, tasks):
    logger.info(f"[DB SAVE] Сохранение задач CE для проекта {project_key} в {db_path}")
    new_count = 0

    with sqlite3.connect(db_path) as conn:
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

    logger.info(f"[DB SAVE] Новых задач добавлено: {new_count}")
    return new_count


def get_total_runs_from_db(db_path, project_key):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ce_tasks_history WHERE project_key = ?", (project_key,))
        total = cur.fetchone()[0]

    logger.info(f"[DB READ] Полное число запусков для {project_key}: {total}")
    return total


def get_sonar_users(inst):
    logger.info(f"[{inst['name']}][USERS] Получение списка пользователей...")
    users = []
    page = 1
    size = 500

    while True:
        logger.info(f"[{inst['name']}][USERS] GET page={page}")

        r = inst["session"].get(
            f"{inst['url']}/api/users/search",
            params={"p": page, "ps": size},
            verify=False
        )
        r.raise_for_status()

        data = r.json()
        batch = data.get("users", [])
        users.extend(batch)

        logger.info(f"[{inst['name']}][USERS] Получено {len(batch)}, всего: {len(users)}")

        if page * size >= data.get("paging", {}).get("total", 0):
            break

        page += 1

    return users


def get_projects(inst):
    logger.info(f"[{inst['name']}][PROJECTS] Получение проектов...")
    projects = []
    page = 1
    size = 500

    while True:
        logger.info(f"[{inst['name']}][PROJECTS] GET page={page}")

        r = inst["session"].get(
            f"{inst['url']}/api/projects/search",
            params={"p": page, "ps": size},
            verify=False
        )
        r.raise_for_status()

        data = r.json()
        batch = data.get("components", [])
        projects.extend(batch)

        logger.info(f"[{inst['name']}][PROJECTS] Получено {len(batch)}, всего: {len(projects)}")

        if page * size >= data.get("paging", {}).get("total", 0):
            break

        page += 1

    return projects


def get_ncloc(inst, project_key):
    logger.info(f"[{inst['name']}][NCLOC] {project_key}")

    r = inst["session"].get(
        f"{inst['url']}/api/measures/component",
        params={"component": project_key, "metricKeys": "ncloc"},
        verify=False
    )
    r.raise_for_status()

    measures = r.json().get("component", {}).get("measures", [])
    n = int(measures[0].get("value", 0)) if measures else 0

    logger.info(f"[{inst['name']}][NCLOC] {project_key}: {n}")
    return n


def get_issues_count(inst, project_key):
    logger.info(f"[{inst['name']}][ISSUES] {project_key}")

    r = inst["session"].get(
        f"{inst['url']}/api/issues/search",
        params={"componentKeys": project_key, "ps": 1},
        verify=False
    )
    r.raise_for_status()

    total = r.json().get("total", 0)
    logger.info(f"[{inst['name']}][ISSUES] {project_key}: {total}")
    return total


def get_ce_tasks(inst, project_key):
    logger.info(f"[{inst['name']}][CE TASKS] Получение задач CE для {project_key}")

    tasks_all = []
    page = 1
    size = 100

    while True:
        logger.info(f"[{inst['name']}][CE TASKS] GET page={page}")

        r = inst["session"].get(
            f"{inst['url']}/api/ce/activity",
            params={
                "status": "IN_PROGRESS,SUCCESS,FAILED,CANCELED",
                "component": project_key,
                "p": page, "ps": size
            },
            verify=False
        )
        r.raise_for_status()

        data = r.json()
        batch = data.get("tasks", [])
        tasks_all.extend(batch)

        logger.info(f"[{inst['name']}][CE TASKS] Получено {len(batch)}, всего {len(tasks_all)}")

        if page * size >= data.get("paging", {}).get("total", 0):
            break

        page += 1

    return len(tasks_all), tasks_all


def write_instance_report(workbook, inst, users, projects):
    prefix = inst["name"]
    logger.info(f"[{prefix}] Формирование листов Excel...")

    ws_users = workbook.add_worksheet(f"{prefix}_users")
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

    ws_projects = workbook.add_worksheet(f"{prefix}_projects")
    headers_projects = [
        "project_name",
        "lines_of_code",
        "issues_total",
        "run_count_total"
    ]
    for col, h in enumerate(headers_projects):
        ws_projects.write(0, col, h)

    ws_summary = workbook.add_worksheet(f"{prefix}_summary")

    total_runs = 0

    for i, p in enumerate(projects, start=1):
        key = p.get("key")
        name = p.get("name")

        logger.info(f"[{prefix}] === [{i}/{len(projects)}] Проект: {key} ===")

        ncloc = get_ncloc(inst, key)
        issues = get_issues_count(inst, key)

        _, tasks = get_ce_tasks(inst, key)
        save_ce_tasks_to_db(inst["db_path"], key, tasks)

        runs_total = get_total_runs_from_db(inst["db_path"], key)
        total_runs += runs_total

        ws_projects.write(i, 0, name)
        ws_projects.write(i, 1, ncloc)
        ws_projects.write(i, 2, issues)
        ws_projects.write(i, 3, runs_total)

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

    logger.info(f"[{prefix}] Листы Excel готовы")


def main():
    logger.info("==== START ====")

    instances = load_instances()
    workbook = xlsxwriter.Workbook("sonar_report.xlsx")

    for inst in instances:
        logger.info(f"[{inst['name']}] Начинаем обработку …")
        init_db(inst["db_path"])
        users = get_sonar_users(inst)
        projects = get_projects(inst)
        write_instance_report(workbook, inst, users, projects)
        logger.info(f"[{inst['name']}] Готово")

    workbook.close()
    logger.info("Готово: sonar_report.xlsx")
    logger.info("==== DONE ====")


if __name__ == "__main__":
    main()
