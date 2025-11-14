import os
import time
import logging
import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# ========================= CONFIG =========================
load_dotenv()

GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")

OUTPUT_FILE = "grafana_report.xlsx"
LOG_FILE = "grafana_cookie_report.log"

ORG_LIMIT = 5
SLEEP_AFTER_SWITCH = 1
SLEEP_BETWEEN_CALLS = 0.2

# ========================= LOGGING =========================
logger = logging.getLogger("grafana_cookie_report")
logger.setLevel(logging.INFO)

fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

requests.packages.urllib3.disable_warnings()


# ========================= SESSION + COOKIE LOGIN =========================

session = requests.Session()
session.verify = False

def login_cookie():
    payload = {"user": GRAFANA_USER, "password": GRAFANA_PASS}
    r = session.post(f"{GRAFANA_URL}/login", json=payload)
    r.raise_for_status()
    logger.info("Успешный вход, cookie установлены: " + str(session.cookies.get_dict()))
    time.sleep(SLEEP_BETWEEN_CALLS)

login_cookie()


# ========================= API CALLS =========================

def switch_org(org_id):
    r = session.post(f"{GRAFANA_URL}/api/user/using/{org_id}")
    r.raise_for_status()
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

def get_dashboards_in_folder(fid):
    r = session.get(f"{GRAFANA_URL}/api/search",
                    params={"folderIds": fid, "type": "dash-db", "limit": 5000})
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json()

def get_dashboards_root():
    r = session.get(f"{GRAFANA_URL}/api/search",
                    params={"folderIds": 0, "type": "dash-db", "limit": 5000})
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


# ========================= MAIN =========================

logger.info("Получаю организации...")
orgs = get_orgs()
orgs = orgs[:ORG_LIMIT]

rows_summary = []
rows_users = []
rows_folders = []
rows_dash = []

for org in tqdm(orgs, desc="Организации", ncols=90):

    org_id = org["id"]
    org_name = org["name"]

    logger.info(f"Переключение в орг: {org_name} ({org_id})")
    switch_org(org_id)

    # --- USERS ---
    users = get_users_in_org(org_id)
    for u in users:
        rows_users.append({
            "org_id": org_id,
            "org_name": org_name,
            "user_id": u.get("userId"),
            "email": u.get("email"),
            "login": u.get("login"),
            "role": u.get("role")
        })

    # --- FOLDERS ---
    folders = get_folders()
    dashboards_total = 0
    panels_total = 0

    for f in tqdm(folders, desc=f"Папки {org_name}", leave=False, ncols=90):
        fid = f["id"]
        fname = f["title"]

        dashboards = get_dashboards_in_folder(fid)
        dashboards_total += len(dashboards)

        rows_folders.append({
            "org_id": org_id,
            "org_name": org_name,
            "folder_id": fid,
            "folder_title": fname,
            "dashboards_count": len(dashboards)
        })

        for d in dashboards:
            uid = d["uid"]
            title = d["title"]
            panels = get_dashboard_panels(uid)
            panels_total += panels

            rows_dash.append({
                "org_id": org_id,
                "org_name": org_name,
                "folder_id": fid,
                "folder_title": fname,
                "dashboard_uid": uid,
                "dashboard_title": title,
                "panels": panels,
            })

    # --- ROOT DASHBOARDS ---
    root_dash = get_dashboards_root()
    dashboards_total += len(root_dash)

    for d in root_dash:
        uid = d["uid"]
        title = d["title"]
        panels = get_dashboard_panels(uid)
        panels_total += panels

        rows_dash.append({
            "org_id": org_id,
            "org_name": org_name,
            "folder_id": 0,
            "folder_title": "ROOT",
            "dashboard_uid": uid,
            "dashboard_title": title,
            "panels": panels,
        })

    rows_summary.append({
        "org_id": org_id,
        "org_name": org_name,
        "users_total": len(users),
        "folders_total": len(folders),
        "dashboards_total": dashboards_total,
        "panels_total": panels_total
    })


# ========================= EXPORT =========================

df_summary = pd.DataFrame(rows_summary)
df_users = pd.DataFrame(rows_users)
df_folders = pd.DataFrame(rows_folders)
df_dashboards = pd.DataFrame(rows_dash)

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df_summary.to_excel(writer, sheet_name="Summary", index=False)
    df_users.to_excel(writer, sheet_name="Users", index=False)
    df_folders.to_excel(writer, sheet_name="Folders", index=False)
    df_dashboards.to_excel(writer, sheet_name="Dashboards", index=False)

logger.info(f"Готово! Данные сохранены в {OUTPUT_FILE}")
