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

LOG_FILE = "grafana_report.log"
OUTPUT_FILE = "grafana_report.xlsx"

ORG_LIMIT = 5  # ограничение организаций
SLEEP_AFTER_SWITCH = 1.0  # пауза после POST /api/user/using
SLEEP_BETWEEN_CALLS = 0.2  # пауза между GET

# ========================= LOGGING =========================
logger = logging.getLogger("grafana_report")
logger.setLevel(logging.INFO)

fmt = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
)
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


# ========================= API FUNCTIONS =========================


def switch_org(org_id):
    r = session.post(f"{GRAFANA_URL}/api/user/using/{org_id}")
    if r.status_code != 200:
        raise Exception(f"Не удалось переключиться в орг {org_id}: {r.text}")
    time.sleep(SLEEP_AFTER_SWITCH)


def get_orgs():
    r = session.get(f"{GRAFANA_URL}/api/orgs")
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json()


def get_users_in_org(org_id):
    r = session.get(f"{GRAFANA_URL}/api/orgs/{org_id}/users")
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
        params={"folderIds": folder_id, "type": "dash-db", "limit": 5000},
    )
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json()


def get_dashboards_root():
    r = session.get(
        f"{GRAFANA_URL}/api/search",
        params={"folderIds": 0, "type": "dash-db", "limit": 5000},
    )
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json()


def get_dashboard_panels(uid):
    r = session.get(f"{GRAFANA_URL}/api/dashboards/uid/{uid}")
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)

    dash = r.json()["dashboard"]
    count = 0

    if "panels" in dash:
        count += len(dash["panels"])

    if "rows" in dash:
        for row in dash["rows"]:
            if "panels" in row:
                count += len(row["panels"])

    return count


# ========================= MAIN LOGIC =========================

logger.info("Получаю организации...")
orgs = get_orgs()
orgs = orgs[:ORG_LIMIT]

logger.info(f"Будут обработаны {len(orgs)} организаций.")

rows_summary = []
rows_users = []
rows_folder_details = []
rows_dashboard_details = []


for org in tqdm(orgs, desc="Организации", ncols=80):
    org_id = org["id"]
    org_name = org["name"]

    logger.info(f"Переключаюсь в организацию: {org_name} (id={org_id})")
    switch_org(org_id)

    # ---- USERS ----
    users = get_users_in_org(org_id)
    for u in users:
        rows_users.append(
            {
                "org_id": org_id,
                "org_name": org_name,
                "user_id": u.get("userId"),
                "email": u.get("email"),
                "login": u.get("login"),
                "role": u.get("role"),
            }
        )

    # ---- FOLDERS ----
    folders = get_folders()
    folder_count = len(folders)

    dashboards_total = 0
    panels_total = 0

    for f in tqdm(folders, desc=f"Папки {org_name}", leave=False, ncols=80):
        folder_id = f["id"]
        folder_title = f["title"]

        dashboards = get_dashboards_in_folder(folder_id)
        dashboards_total += len(dashboards)

        rows_folder_details.append(
            {
                "org_id": org_id,
                "org_name": org_name,
                "folder_id": folder_id,
                "folder_title": folder_title,
                "dashboards_count": len(dashboards),
            }
        )

        for d in dashboards:
            uid = d["uid"]
            dash_title = d["title"]
            panels = get_dashboard_panels(uid)
            panels_total += panels

            rows_dashboard_details.append(
                {
                    "org_id": org_id,
                    "org_name": org_name,
                    "folder_id": folder_id,
                    "folder_title": folder_title,
                    "dashboard_uid": uid,
                    "dashboard_title": dash_title,
                    "panels": panels,
                }
            )

    # ---- ROOT DASHBOARDS ----
    root_dash = get_dashboards_root()
    dashboards_total += len(root_dash)

    for d in root_dash:
        uid = d["uid"]
        title = d["title"]
        panels = get_dashboard_panels(uid)
        panels_total += panels

        rows_dashboard_details.append(
            {
                "org_id": org_id,
                "org_name": org_name,
                "folder_id": 0,
                "folder_title": "ROOT",
                "dashboard_uid": uid,
                "dashboard_title": title,
                "panels": panels,
            }
        )

    # ---- SUMMARY ----
    rows_summary.append(
        {
            "org_id": org_id,
            "org_name": org_name,
            "users_total": len(users),
            "folders_total": folder_count,
            "dashboards_total": dashboards_total,
            "panels_total": panels_total,
        }
    )


# ========================= SAVE =========================

df_summary = pd.DataFrame(rows_summary)
df_users = pd.DataFrame(rows_users)
df_folders = pd.DataFrame(rows_folder_details)
df_dash = pd.DataFrame(rows_dashboard_details)

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df_summary.to_excel(writer, sheet_name="Summary", index=False)
    df_users.to_excel(writer, sheet_name="Users", index=False)
    df_folders.to_excel(writer, sheet_name="Folders", index=False)
    df_dash.to_excel(writer, sheet_name="Dashboards", index=False)

logger.info("🎉 Готово! Отчёт сохранён → " + OUTPUT_FILE)
