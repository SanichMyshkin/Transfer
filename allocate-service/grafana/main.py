import os
import time
import logging
import pandas as pd
from dotenv import load_dotenv
import requests
from tqdm import tqdm

# ========================= CONFIG =========================
load_dotenv()

GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")

LOG_FILE = os.getenv("LOG_FILE", "grafana_report.log")
OUTPUT_FILE = "grafana_report.xlsx"

# Ограничение количества организаций
ORG_LIMIT = 5

# Паузы
SLEEP_AFTER_SWITCH = 1.0      # после POST /api/user/using
SLEEP_BETWEEN_CALLS = 0.20    # между GET запросами

# ========================= LOGGING =========================
logger = logging.getLogger("grafana_report")
logger.setLevel(logging.INFO)

fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

# ========================= SESSION =========================
requests.packages.urllib3.disable_warnings()
session = requests.Session()
session.auth = (GRAFANA_USER, GRAFANA_PASS)
session.verify = False

# ========================= API WRAPPERS =========================

def switch_org(org_id):
    r = session.post(f"{GRAFANA_URL}/api/user/using/{org_id}")
    if r.status_code != 200:
        raise Exception(f"Не удалось переключиться в организацию {org_id}: {r.text}")
    time.sleep(SLEEP_AFTER_SWITCH)


def get_orgs():
    r = session.get(f"{GRAFANA_URL}/api/orgs")
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json()


def get_folders():
    r = session.get(f"{GRAFANA_URL}/api/folders", params={"limit": 5000})
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json()


def get_dashboards_in_folder(folder_id):
    r = session.get(
        f"{GRAFANA_URL}/api/search",
        params={"type": "dash-db", "folderIds": folder_id, "limit": 5000}
    )
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json()


def get_dashboards_root():
    r = session.get(
        f"{GRAFANA_URL}/api/search",
        params={"type": "dash-db", "folderIds": 0, "limit": 5000}
    )
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json()


def get_dashboard_panels(uid):
    r = session.get(f"{GRAFANA_URL}/api/dashboards/uid/{uid}")
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)

    data = r.json()["dashboard"]

    count = 0

    # panels array
    if "panels" in data:
        count += len(data["panels"])

    # rows array (старый формат)
    if "rows" in data:
        for row in data["rows"]:
            if "panels" in row:
                count += len(row["panels"])

    return count


# ========================= MAIN LOGIC =========================

logger.info("📥 Получаю список организаций...")
orgs = get_orgs()

logger.info(f"Найдено организаций: {len(orgs)}. Будет обработано: {ORG_LIMIT}")

orgs = orgs[:ORG_LIMIT]

rows_org_stats = []
rows_folder_details = []
rows_dashboard_details = []

# progress bar
for org in tqdm(orgs, desc="Организации", ncols=80):

    org_id = org["id"]
    org_name = org["name"]

    logger.info(f"===== Организация: {org_name} (id={org_id}) =====")

    # переключаемся
    switch_org(org_id)

    # папки
    folders = get_folders()
    folder_count = len(folders)

    dashboards_total = 0
    panels_total = 0

    # проходим по папкам
    for f in tqdm(folders, desc=f"Папки {org_name}", ncols=80, leave=False):
        folder_id = f["id"]
        folder_title = f["title"]

        dashboards = get_dashboards_in_folder(folder_id)
        dashboards_total += len(dashboards)

        rows_folder_details.append({
            "org_id": org_id,
            "org_name": org_name,
            "folder_id": folder_id,
            "folder_title": folder_title,
            "dashboards_count": len(dashboards)
        })

        for d in dashboards:
            uid = d["uid"]
            dash_title = d["title"]

            panels = get_dashboard_panels(uid)
            panels_total += panels

            rows_dashboard_details.append({
                "org_id": org_id,
                "org_name": org_name,
                "folder_id": folder_id,
                "folder_title": folder_title,
                "dashboard_uid": uid,
                "dashboard_title": dash_title,
                "panels": panels
            })

    # корневые дашборды (folderIds=0)
    root_dash = get_dashboards_root()
    dashboards_total += len(root_dash)

    for d in root_dash:
        uid = d["uid"]
        dash_title = d["title"]

        panels = get_dashboard_panels(uid)
        panels_total += panels

        rows_dashboard_details.append({
            "org_id": org_id,
            "org_name": org_name,
            "folder_id": 0,
            "folder_title": "ROOT",
            "dashboard_uid": uid,
            "dashboard_title": dash_title,
            "panels": panels
        })

    # summary по организации
    rows_org_stats.append({
        "org_id": org_id,
        "org_name": org_name,
        "folders_total": folder_count,
        "dashboards_total": dashboards_total,
        "panels_total": panels_total
    })


# ========================= EXPORT TO EXCEL =========================

df_orgs = pd.DataFrame(rows_org_stats)
df_folders = pd.DataFrame(rows_folder_details)
df_dashboards = pd.DataFrame(rows_dashboard_details)

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df_orgs.to_excel(writer, sheet_name="OrgStats", index=False)
    df_folders.to_excel(writer, sheet_name="Folders", index=False)
    df_dashboards.to_excel(writer, sheet_name="Dashboards", index=False)

logger.info("🎉 Готово! Отчёт сохранён: " + OUTPUT_FILE)
