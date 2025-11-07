import os
import sys
import logging
import urllib3
import xlsxwriter
from datetime import datetime
from dotenv import load_dotenv
from jenkins_groovy import JenkinsGroovyClient
from jenkins_scripts import SCRIPT_USERS, SCRIPT_JOBS, SCRIPT_NODES

# === Настройка логирования ===
LOG_FILE = os.path.join(os.getcwd(), "jenkins_inventory.log")

# сброс старых хендлеров
for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# === Параметры подключения ===
JENKINS_URL = os.getenv("JENKINS_URL")
USER = os.getenv("USER")
TOKEN = os.getenv("TOKEN")
FILE_PATH = os.path.join(os.getcwd(), "jenkins_inventory.xlsx")

client = JenkinsGroovyClient(JENKINS_URL, USER, TOKEN, is_https=True)


# === Получение данных ===
def get_users():
    logger.info("Получаем пользователей...")
    data = client.run_script(SCRIPT_USERS)
    logger.info(f"Пользователей: {data['total']}")
    return data


def get_jobs():
    logger.info("Получаем джобы...")
    data = client.run_script(SCRIPT_JOBS)
    logger.info(f"Джоб: {data['total']}")
    return data


def get_nodes():
    logger.info("Получаем ноды...")
    data = client.run_script(SCRIPT_NODES)
    logger.info(f"Нод: {data['total']}")
    return data


# === Запись Excel ===
def write_excel(users, jobs, nodes):
    """Перезаписывает Excel-файл полностью"""
    wb = xlsxwriter.Workbook(FILE_PATH)

    # --- Users ---
    ws_u = wb.add_worksheet("Users")
    headers_u = ["ID", "Full Name", "Email"]
    for col, h in enumerate(headers_u):
        ws_u.write(0, col, h)
    for row, u in enumerate(users["users"], start=1):
        ws_u.write(row, 0, u.get("id", ""))
        ws_u.write(row, 1, u.get("fullName", ""))
        ws_u.write(row, 2, u.get("email", ""))

    # --- Jobs ---
    ws_j = wb.add_worksheet("Jobs")
    headers_j = [
        "Name", "Type", "URL", "Description",
        "Is Buildable", "Is Folder", "Last Build",
        "Last Result", "Last Build Time"
    ]
    for col, h in enumerate(headers_j):
        ws_j.write(0, col, h)
    for row, j in enumerate(jobs["jobs"], start=1):
        ws_j.write(row, 0, j.get("name", ""))
        ws_j.write(row, 1, j.get("type", ""))
        ws_j.write(row, 2, j.get("url", ""))
        ws_j.write(row, 3, j.get("description", ""))
        ws_j.write(row, 4, str(j.get("isBuildable", "")))
        ws_j.write(row, 5, str(j.get("isFolder", "")))
        ws_j.write(row, 6, str(j.get("lastBuild", "")))
        ws_j.write(row, 7, str(j.get("lastResult", "")))
        ws_j.write(row, 8, str(j.get("lastBuildTime", "")))

    # --- Jobs with Builds only ---
    ws_jb = wb.add_worksheet("JobsWithBuilds")
    for col, h in enumerate(headers_j):
        ws_jb.write(0, col, h)

    filtered_jobs = [
        j for j in jobs["jobs"]
        if j.get("lastBuild") not in (None, "", "null")
    ]

    total_builds = 0
    for row, j in enumerate(filtered_jobs, start=1):
        ws_jb.write(row, 0, j.get("name", ""))
        ws_jb.write(row, 1, j.get("type", ""))
        ws_jb.write(row, 2, j.get("url", ""))
        ws_jb.write(row, 3, j.get("description", ""))
        ws_jb.write(row, 4, str(j.get("isBuildable", "")))
        ws_jb.write(row, 5, str(j.get("isFolder", "")))
        ws_jb.write(row, 6, str(j.get("lastBuild", "")))
        ws_jb.write(row, 7, str(j.get("lastResult", "")))
        ws_jb.write(row, 8, str(j.get("lastBuildTime", "")))

        # lastBuild — это номер последнего билда, значит суммарно билдов ≈ lastBuild
        try:
            total_builds += int(j.get("lastBuild", 0))
        except ValueError:
            pass

    logger.info(f"Добавлен лист JobsWithBuilds: {len(filtered_jobs)} записей, всего билдов: {total_builds}")

    # --- Nodes ---
    ws_n = wb.add_worksheet("Nodes")
    headers_n = ["Name", "Online", "Executors", "Labels", "Mode", "Description"]
    for col, h in enumerate(headers_n):
        ws_n.write(0, col, h)
    for row, n in enumerate(nodes["nodes"], start=1):
        ws_n.write(row, 0, n.get("name", ""))
        ws_n.write(row, 1, str(n.get("online", "")))
        ws_n.write(row, 2, str(n.get("executors", "")))
        ws_n.write(row, 3, n.get("labels", ""))
        ws_n.write(row, 4, n.get("mode", ""))
        ws_n.write(row, 5, n.get("description", ""))

    # --- Summary ---
    ws_s = wb.add_worksheet("Summary")
    ws_s.write(0, 0, "Дата")
    ws_s.write(1, 0, "Пользователи")
    ws_s.write(2, 0, "Джобы")
    ws_s.write(3, 0, "Джобы с билдами")
    ws_s.write(4, 0, "Всего билдов")
    ws_s.write(5, 0, "Ноды")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws_s.write(0, 1, now)
    ws_s.write(1, 1, users["total"])
    ws_s.write(2, 1, jobs["total"])
    ws_s.write(3, 1, len(filtered_jobs))
    ws_s.write(4, 1, total_builds)
    ws_s.write(5, 1, nodes["total"])

    wb.close()
    logger.info(f"Excel полностью перезаписан: {FILE_PATH}")




# === Основной поток ===
def main():
    logger.info("=== Старт инвентаризации Jenkins ===")
    try:
        users = get_users()
        jobs = get_jobs()
        nodes = get_nodes()
        write_excel(users, jobs, nodes)
        logger.info("Инвентаризация завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
