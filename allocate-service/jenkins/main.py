import os
import sys
import logging
import urllib3
import openpyxl
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

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
)

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()


# === Глобальные параметры ===
JENKINS_URL = os.getenv("JENKINS_URL")
USER = os.getenv("USER")
TOKEN = os.getenv("TOKEN")
FILE_PATH = os.path.join(os.getcwd(), "jenkins_inventory.xlsx")

client = JenkinsGroovyClient(JENKINS_URL, USER, TOKEN, is_https=True)


# === Вспомогательные функции ===
def create_excel_if_missing():
    """Создать базовый Excel-файл, если отсутствует"""
    if not os.path.exists(FILE_PATH):
        wb = xlsxwriter.Workbook(FILE_PATH)
        wb.add_worksheet("Users")
        wb.add_worksheet("Jobs")
        wb.add_worksheet("Nodes")
        wb.add_worksheet("Summary")
        wb.close()
        logger.info("Создан новый файл jenkins_inventory.xlsx")


def get_users():
    """Получить пользователей из Jenkins"""
    logger.info("Получаем пользователей...")
    data = client.run_script(SCRIPT_USERS)
    logger.info(f"Пользователей: {data['total']}")
    return data


def get_jobs():
    """Получить джобы из Jenkins"""
    logger.info("Получаем джобы...")
    data = client.run_script(SCRIPT_JOBS)
    logger.info(f"Джоб: {data['total']}")
    return data


def get_nodes():
    """Получить ноды из Jenkins"""
    logger.info("Получаем ноды...")
    data = client.run_script(SCRIPT_NODES)
    logger.info(f"Нод: {data['total']}")
    return data


def write_users(ws, users):
    """Записать пользователей в Excel"""
    if ws.max_row == 1 and ws.cell(1, 1).value is None:
        ws.append(["ID", "Full Name", "Email"])
    for u in users["users"]:
        ws.append([u.get("id", ""), u.get("fullName", ""), u.get("email", "")])


def write_jobs(ws, jobs):
    """Записать джобы в Excel"""
    if ws.max_row == 1 and ws.cell(1, 1).value is None:
        ws.append(
            [
                "Timestamp",
                "Name",
                "Type",
                "URL",
                "Description",
                "Is Buildable",
                "Is Folder",
                "Last Build",
                "Last Result",
                "Last Build Time",
            ]
        )
    timestamp = datetime.now().isoformat()
    for j in jobs["jobs"]:
        ws.append(
            [
                timestamp,
                j.get("name", ""),
                j.get("type", ""),
                j.get("url", ""),
                j.get("description", ""),
                str(j.get("isBuildable", "")),
                str(j.get("isFolder", "")),
                str(j.get("lastBuild", "")),
                str(j.get("lastResult", "")),
                str(j.get("lastBuildTime", "")),
            ]
        )


def write_nodes(ws, nodes):
    """Записать ноды в Excel"""
    if ws.max_row == 1 and ws.cell(1, 1).value is None:
        ws.append(
            [
                "Timestamp",
                "Name",
                "Online",
                "Executors",
                "Labels",
                "Mode",
                "Description",
            ]
        )
    timestamp = datetime.now().isoformat()
    for n in nodes["nodes"]:
        ws.append(
            [
                timestamp,
                n.get("name", ""),
                str(n.get("online", "")),
                str(n.get("executors", "")),
                n.get("labels", ""),
                n.get("mode", ""),
                n.get("description", ""),
            ]
        )


def write_summary(ws, users, jobs, nodes):
    """Добавить сводку в Excel"""
    if ws.max_row == 1 and ws.cell(1, 1).value is None:
        ws.append(["Дата", "Пользователи", "Джобы", "Ноды"])
    ws.append(
        [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            users["total"],
            jobs["total"],
            nodes["total"],
        ]
    )


def update_excel(users, jobs, nodes):
    """Обновить Excel-файл всеми данными"""
    create_excel_if_missing()
    wb = openpyxl.load_workbook(FILE_PATH)
    write_users(wb["Users"], users)
    write_jobs(wb["Jobs"], jobs)
    write_nodes(wb["Nodes"], nodes)
    write_summary(wb["Summary"], users, jobs, nodes)
    wb.save(FILE_PATH)
    logger.info(f"Результаты записаны в {FILE_PATH}")


# === Основной поток ===
def main():
    logger.info("=== Старт инвентаризации Jenkins ===")
    try:
        users = get_users()
        jobs = get_jobs()
        nodes = get_nodes()
        update_excel(users, jobs, nodes)
        logger.info("Инвентаризация завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
