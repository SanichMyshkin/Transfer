import os
import time
import logging
import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

from gitlab_config_loader import GitlabConfigLoader

load_dotenv()

GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")

OUTPUT_FILE = "grafana_report.xlsx"
LOG_FILE = "grafana_report.log"

ORG_LIMIT = int(os.getenv("ORG_LIMIT", "5"))
SLEEP_AFTER_SWITCH = 1
SLEEP_BETWEEN_CALLS = 0.2

logger = logging.getLogger("grafana_report")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

requests.packages.urllib3.disable_warnings()

session = requests.Session()
session.verify = False

loader = GitlabConfigLoader()
org_owners = loader.get_org_owners()


def login_cookie():
    payload = {"user": GRAFANA_USER, "password": GRAFANA_PASS}
    r = session.post(f"{GRAFANA_URL}/login", json=payload)
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)


def switch_org(org_id):
    r = session.post(f"{GRAFANA_URL}/api/user/using/{org_id}")
    if r.status_code == 401:
        raise PermissionError("NO ACCESS")
    r.raise_for_status()
    time.sleep(SLEEP_AFTER_SWITCH)


def get_orgs():
    r = session.get(f"{GRAFANA_URL}/api/orgs")
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json()


def get_grafana_users():
    users = []
    page = 1
    while True:
        r = session.get(f"{GRAFANA_URL}/api/users", params={"page": page, "limit": 500})
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        users.extend(chunk)
        if len(chunk) < 500:
            break
        page += 1
    time.sleep(SLEEP_BETWEEN_CALLS)
    return users


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
    r = session.get(
        f"{GRAFANA_URL}/api/search",
        params={"folderIds": fid, "type": "dash-db", "limit": 5000},
    )
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json()


def get_all_dashboards_raw():
    r = session.get(
        f"{GRAFANA_URL}/api/search", params={"type": "dash-db", "limit": 5000}
    )
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json()


def get_root_dashboards():
    all_dash = get_all_dashboards_raw()
    return [d for d in all_dash if d.get("folderId") in (0, None)]


def get_dashboard_panels(uid):
    r = session.get(f"{GRAFANA_URL}/api/dashboards/uid/{uid}")
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    dash = r.json()["dashboard"]
    c = 0
    if "panels" in dash:
        c += len(dash["panels"])
    if "rows" in dash:
        for row in dash["rows"]:
            if "panels" in row:
                c += len(row["panels"])
    return c


login_cookie()

logger.info("Загружаю список организаций...")
orgs = get_orgs()
orgs = orgs[:ORG_LIMIT]

rows_grafana_users = []
rows_org_users = []
rows_folders = []
rows_dashboards = []
rows_orgs = []

grafana_users = get_grafana_users()
rows_grafana_users = grafana_users

for org in tqdm(orgs, desc="Организации", ncols=100):
    org_id = org["id"]
    org_name = org["name"]

    owners = org_owners.get(org_id, [])
    owners_str = ", ".join([f"{o[0]} (admin={o[1]})" for o in owners]) if owners else "NO OWNER"

    try:
        switch_org(org_id)
    except PermissionError:
        rows_orgs.append(
            {
                "org_name": org_name,
                "owners": owners_str,
                "users_total": "NO ACCESS",
                "folders_total": "NO ACCESS",
                "dashboards_total": "NO ACCESS",
                "panels_total": "NO ACCESS",
            }
        )
        continue

    users = get_users_in_org(org_id)
    for u in users:
        rows_org_users.append(
            {
                "org_name": org_name,
                "user_id": u.get("userId"),
                "email": u.get("email"),
                "login": u.get("login"),
                "role": u.get("role"),
            }
        )

    folders = get_folders()
    dashboards_total = 0
    panels_total = 0

    for f in tqdm(folders, desc=f"Папки {org_name}", leave=False, ncols=100):
        fid = f["id"]
        fname = f["title"]

        dashboards = get_dashboards_in_folder(fid)
        dashboards_total += len(dashboards)

        rows_folders.append(
            {
                "org_name": org_name,
                "folder_id": fid,
                "folder_title": fname,
                "dashboards_count": len(dashboards),
            }
        )

        for d in dashboards:
            uid = d["uid"]
            title = d["title"]
            panels = get_dashboard_panels(uid)
            panels_total += panels

            rows_dashboards.append(
                {
                    "org_name": org_name,
                    "folder_id": fid,
                    "folder_title": fname,
                    "dashboard_uid": uid,
                    "dashboard_title": title,
                    "panels": panels,
                }
            )

    root_dash = get_root_dashboards()
    dashboards_total += len(root_dash)

    for d in root_dash:
        uid = d["uid"]
        title = d["title"]
        panels = get_dashboard_panels(uid)
        panels_total += panels

        rows_dashboards.append(
            {
                "org_name": org_name,
                "folder_id": 0,
                "folder_title": "ROOT",
                "dashboard_uid": uid,
                "dashboard_title": title,
                "panels": panels,
            }
        )

    rows_orgs.append(
        {
            "org_name": org_name,
            "owners": owners_str,
            "users_total": len(users),
            "folders_total": len(folders),
            "dashboards_total": dashboards_total,
            "panels_total": panels_total,
        }
    )

df_grafana_users = pd.DataFrame(rows_grafana_users)
df_org_users = pd.DataFrame(rows_org_users)
df_orgs = pd.DataFrame(rows_orgs)
df_folders = pd.DataFrame(rows_folders)
df_dashboards = pd.DataFrame(rows_dashboards)

global_summary = {
    "organizations_total": len(df_orgs),
    "users_total": len(df_grafana_users),
    "folders_total": len(df_folders),
    "dashboards_total": len(df_dashboards),
    "panels_total": df_dashboards["panels"].sum() if not df_dashboards.empty else 0,
}

df_global_summary = pd.DataFrame(list(global_summary.items()), columns=["metric", "value"])

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df_grafana_users.to_excel(writer, sheet_name="GrafanaUsers", index=False)
    df_org_users.to_excel(writer, sheet_name="OrganizationUsers", index=False)
    df_orgs.to_excel(writer, sheet_name="Organizations", index=False)
    df_folders.to_excel(writer, sheet_name="Folders", index=False)
    df_dashboards.to_excel(writer, sheet_name="Dashboards", index=False)
    df_global_summary.to_excel(writer, sheet_name="Summary", index=False)

logger.info(f"Готово! Данные сохранены: {OUTPUT_FILE}")
