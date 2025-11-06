import os
import json
import urllib3
import logging
import openpyxl
import xlsxwriter
from datetime import datetime
from dotenv import load_dotenv
from jenkins_groovy import JenkinsGroovyClient
from jenkins_scripts import SCRIPT_USERS, SCRIPT_JOBS, SCRIPT_NODES

# === Настройка логирования ===
LOG_FILE = os.path.join(os.getcwd(), "jenkins_inventory.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

JENKINS_URL = os.getenv("JENKINS_URL")
USER = os.getenv("USER")
TOKEN = os.getenv("TOKEN")

client = JenkinsGroovyClient(JENKINS_URL, USER, TOKEN, is_https=True)

logging.info("=== Запуск инвентаризации Jenkins ===")

try:
    users = client.run_script(SCRIPT_USERS)
    jobs = client.run_script(SCRIPT_JOBS)
    nodes = client.run_script(SCRIPT_NODES)
    logging.info(
        f"Получено: {users['total']} пользователей, {jobs['total']} джоб, {nodes['total']} нод"
    )
except Exception as e:
    logging.error(f"Ошибка при выполнении Groovy-скриптов: {e}")
    raise

FILE_PATH = os.path.join(os.getcwd(), "jenkins_inventory.xlsx")


def create_excel_if_missing():
    if not os.path.exists(FILE_PATH):
        wb = xlsxwriter.Workbook(FILE_PATH)
        wb.add_worksheet("Users")
        wb.add_worksheet("Jobs")
        wb.add_worksheet("Nodes")
        wb.add_worksheet("Summary")
        wb.close()
        logging.info("Создан новый файл jenkins_inventory.xlsx")


create_excel_if_missing()
wb = openpyxl.load_workbook(FILE_PATH)
now = datetime.now().isoformat()

# --- Users ---
ws_u = wb["Users"]
if ws_u.max_row == 1 and ws_u.cell(1, 1).value is None:
    ws_u.append(["Timestamp", "ID", "Full Name", "Email"])
for u in users["users"]:
    ws_u.append([now, u.get("id", ""), u.get("fullName", ""), u.get("email", "")])

# --- Jobs ---
ws_j = wb["Jobs"]
if ws_j.max_row == 1 and ws_j.cell(1, 1).value is None:
    ws_j.append(
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
for j in jobs["jobs"]:
    ws_j.append(
        [
            now,
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

# --- Nodes ---
ws_n = wb["Nodes"]
if ws_n.max_row == 1 and ws_n.cell(1, 1).value is None:
    ws_n.append(
        ["Timestamp", "Name", "Online", "Executors", "Labels", "Mode", "Description"]
    )
for n in nodes["nodes"]:
    ws_n.append(
        [
            now,
            n.get("name", ""),
            str(n.get("online", "")),
            str(n.get("executors", "")),
            n.get("labels", ""),
            n.get("mode", ""),
            n.get("description", ""),
        ]
    )

# --- Summary ---
ws_s = wb["Summary"]
if ws_s.max_row == 1 and ws_s.cell(1, 1).value is None:
    ws_s.append(["Timestamp", "Users", "Jobs", "Nodes", "Total"])
ws_s.append(
    [
        now,
        users["total"],
        jobs["total"],
        nodes["total"],
        users["total"] + jobs["total"] + nodes["total"],
    ]
)

wb.save(FILE_PATH)
logging.info(f"Инвентаризация завершена, результаты добавлены в {FILE_PATH}")
