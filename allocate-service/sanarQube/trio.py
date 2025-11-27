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


def create_instance(name, url, token, db_suffix):
    session = requests.Session()
    session.auth = (token, "")
    return {
        "name": name,
        "url": url.rstrip("/"),
        "session": session,
        "db_path": f"sonar_history_{db_suffix}.db",
    }


def load_instances():
    instances = []

    base_url = os.getenv("SONAR_URL")
    base_token = os.getenv("SONAR_TOKEN")
    if base_url and base_token:
        instances.append(create_instance("sonar1", base_url, base_token, "1"))

    for idx in (2, 3):
        url = os.getenv(f"SONAR{idx}_URL")
        token = os.getenv(f"SONAR{idx}_TOKEN")
        if url and token:
            instances.append(create_instance(f"sonar{idx}", url, token, str(idx)))

    if not instances:
        logger.error("Не заданы переменные окружения для ни одного SonarQube")
        raise SystemExit(1)

    logger.info(f"Найдено инстансов SonarQube: {len(instances)}")
    for inst in instances:
        logger.info(f"Инстанс: {inst['name']} url={inst['url']} db={inst['db_path']}")

    return instances


def init_db(db_path):
    logger.info(f"Инициализация базы данных {db_path}...")
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
    logger.info(f"База данных {db_path} готова.")


def save_ce_tasks_to_db(db_path, project_key, tasks):
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
    logger.info(f"[DB:{db_path}] Новых задач добавлено: {new_count}")
    return new_count


def get_total_runs_from_db(db_path, project_key):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ce_tasks_history WHERE project_key = ?", (project_key,))
        total = cur.fetchone()[0]
    logger.info(f"[DB:{db_path}] Полное количество запусков проекта {project_key}: {total}")
    return total


def get_sonar_users(instance):
    logger.info(f"[{instance['name']}] Получение списка пользователей...")
    users = []
    page = 1
    size = 500
    while True:
        logger.info(f"[{instance['name']}][USERS] GET page={page}")
        r = instance["session"].get(
            f"{instance['url']}/api/users/search",
            params={"p": page, "ps": size},
            verify=False
        )
        r.raise_for_status()
        data = r.json()
        batch = data.get("users", [])
        users.extend(batch)
        logger.info(f"[{instance['name']}][USERS] Получено {len(batch)} (всего: {len(users)})")
        if page * size >= data.get("paging", {}).get("total", 0):
            break
        page += 1
    logger.info(f"[{instance['name']}] Пользователи получены: {len(users)}")
    return users


def get_projects(instance):
    logger.info(f"[{instance['name']}] Получение проектов...")
    projects = []
    page = 1
    size = 500
    while True:
        logger.info(f"[{instance['name']}][PROJECTS] GET page={page}")
        r = instance["session"].get(
            f"{instance['url']}/api/projects/search",
            params={"p": page, "ps": size},
            verify=False
        )
        r.raise_for_status()
        data = r.json()
        batch = data.get("components", [])
        projects.extend(batch)
        logger.info(f"[{instance['name']}][PROJECTS] Получено {len(batch)} (всего: {len(projects)})")
        if page * size >= data.get("paging", {}).get("total", 0):
            break
        page += 1
    logger.info(f"[{instance['name']}] Проекты получены: {len(projects)}")
    return projects


def get_ncloc(instance, project_key):
    logger.info(f"[{instance['name']}][NCLOC] Получение строк кода для {project_key}")
    r = instance["session"].get(
        f"{instance['url']}/api/measures/component",
        params={"component": project_key, "metricKeys": "ncloc"},
        verify=False
    )
    r.raise_for_status()
    measures = r.json().get("component", {}).get("measures", [])
    n = int(measures[0].get("value", 0)) if measures else 0
    logger.info(f"[{instance['name']}][NCLOC] {project_key}: {n}")
    return n


def get_issues_count(instance, project_key):
    r = instance["session"].get(
        f"{instance['url']}/api/issues/search",
        params={"componentKeys": project_key, "ps": 1},
        verify=False
    )
    r.raise_for_status()
    total = r.json().get("total", 0)
    logger.info(f"[{instance['name']}][ISSUES] {project_key}: {total}")
    return total


def get_ce_tasks(instance, project_key):
    logger.info(f"[{instance['name']}][TASKS] Получение CE задач для {project_key}")
    page = 1
    size = 100
    tasks_all = []
    while True:
        logger.info(f"[{instance['name']}][TASKS] GET page={page}")
        r = instance["session"].get(
            f"{instance['url']}/api/ce/activity",
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
        logger.info(f"[{instance['name']}][TASKS] Получено {len(batch)} (всего: {len(tasks_all)})")
        if page * size >= data.get("paging", {}).get("total", 0):
            break
        page += 1
    return len(tasks_all), tasks_all


def write_instance_report(workbook, instance, users, projects):
    prefix = instance["name"]
    logger.info(f"[{prefix}] Формирование листов в Excel...")

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

        logger.info(f"[{prefix}] === [{i}/{len(projects)}] Обработка проекта: {key} ===")

        ncloc = get_ncloc(instance, key)
        issues = get_issues_count(instance, key)

        _, tasks = get_ce_tasks(instance, key)
        save_ce_tasks_to_db(instance["db_path"], key, tasks)

        runs_total = get_total_runs_from_db(instance["db_path"], key)
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

    logger.info(f"[{prefix}] Листы Excel сформированы.")


def main():
    logger.info("==== START ====")

    instances = load_instances()
    workbook = xlsxwriter.Workbook("sonar_report.xlsx")

    for inst in instances:
        init_db(inst["db_path"])
        users = get_sonar_users(inst)
        projects = get_projects(inst)
        write_instance_report(workbook, inst, users, projects)

    workbook.close()
    logger.info("Отчёт sonar_report.xlsx сформирован.")
    logger.info("==== DONE ====")


if __name__ == "__main__":
    main()
