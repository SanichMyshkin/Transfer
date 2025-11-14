import os
import time
import logging
import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

from gitlab_config_loader import GitLabConfigLoader

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

fmt = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
)
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

requests.packages.urllib3.disable_warnings()
session = requests.Session()
session.verify = False


def login_cookie():
    r = session.post(
        f"{GRAFANA_URL}/login",
        json={"user": GRAFANA_USER, "password": GRAFANA_PASS},
    )
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)


def switch_org(org_id):
    r = session.post(f"{GRAFANA_URL}/api/user/using/{org_id}")
    if r.status_code == 401:
        return False
    r.raise_for_status()
    time.sleep(SLEEP_AFTER_SWITCH)
    return True


def get_org_name(org_id):
    r = session.get(f"{GRAFANA_URL}/api/orgs/{org_id}")
    if r.status_code != 200:
        return f"ORG_{org_id}"
    return r.json().get("name") or f"ORG_{org_id}"


def get_users_in_org(org_id):
    r = session.get(f"{GRAFANA_URL}/api/orgs/{org_id}/users")
    if r.status_code == 401:
        return None
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


def get_all_grafana_users():
    users = []
    page = 1
    while True:
        r = session.get(
            f"{GRAFANA_URL}/api/users",
            params={"page": page, "limit": 1000},
        )
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        users.extend(data)
        if len(data) < 1000:
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_CALLS)
    return users


login_cookie()

loader = GitLabConfigLoader()
owners_clean = loader.get_owners_clean()

org_ids = sorted(owners_clean.keys())
org_ids = org_ids[:ORG_LIMIT]

rows_all_users = []
for u in get_all_grafana_users():
    rows_all_users.append(
        {
            "id": u.get("id"),
            "name": u.get("name"),
            "login": u.get("login"),
            "email": u.get("email"),
            "isAdmin": u.get("isGrafanaAdmin"),
            "isDisabled": u.get("isDisabled"),
            "created": u.get("createdAt"),
            "updated": u.get("updatedAt"),
        }
    )

rows_users = []
rows_folders = []
rows_dashboards = []
rows_orgs = []

for org_id in tqdm(org_ids, desc="Организации", ncols=100):
    owner_entries = owners_clean.get(org_id, [])
    if isinstance(owner_entries, list):
        owner_group = ", ".join([e[0] for e in owner_entries]) if owner_entries else ""
    elif isinstance(owner_entries, tuple):
        owner_group = owner_entries[0]
    else:
        owner_group = ""

    org_name = get_org_name(org_id)
    ok = switch_org(org_id)

    if not ok:
        rows_orgs.append(
            {
                "organization": org_name,
                "owner_group": owner_group,
                "users_total": "NO ACCESS",
                "folders_total": "NO ACCESS",
                "dashboards_total": "NO ACCESS",
                "panels_total": "NO ACCESS",
            }
        )
        continue

    users = get_users_in_org(org_id)
    if users is None:
        rows_orgs.append(
            {
                "organization": org_name,
                "owner_group": owner_group,
                "users_total": "NO ACCESS",
                "folders_total": "NO ACCESS",
                "dashboards_total": "NO ACCESS",
                "panels_total": "NO ACCESS",
            }
        )
        continue

    users_total = len(users)
    for u in users:
        rows_users.append(
            {
                "organization": org_name,
                "user_id": u.get("userId"),
                "email": u.get("email"),
                "login": u.get("login"),
                "role": u.get("role"),
            }
        )

    folders = get_folders()
    folders_total = len(folders)

    dashboards_total = 0
    panels_total = 0

    for f in folders:
        fid = f["id"]
        fname = f["title"]
        dashboards = get_dashboards_in_folder(fid)
        dashboards_total += len(dashboards)

        rows_folders.append(
            {
                "organization": org_name,
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
                    "organization": org_name,
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
                "organization": org_name,
                "folder_id": 0,
                "folder_title": "ROOT",
                "dashboard_uid": uid,
                "dashboard_title": title,
                "panels": panels,
            }
        )

    rows_orgs.append(
        {
            "organization": org_name,
            "owner_group": owner_group,
            "users_total": users_total,
            "folders_total": folders_total,
            "dashboards_total": dashboards_total,
            "panels_total": panels_total,
        }
    )

df_all_users = pd.DataFrame(rows_all_users)
df_users = pd.DataFrame(rows_users)
df_orgs = pd.DataFrame(rows_orgs)
df_folders = pd.DataFrame(rows_folders)
df_dashboards = pd.DataFrame(rows_dashboards)

global_summary = {
    "organizations_total": len(df_orgs),
    "users_total": len(df_users),
    "folders_total": len(df_folders),
    "dashboards_total": len(df_dashboards),
    "panels_total": df_dashboards["panels"].sum() if not df_dashboards.empty else 0,
}

df_global_summary = pd.DataFrame(
    list(global_summary.items()), columns=["metric", "value"]
)

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df_all_users.to_excel(writer, sheet_name="GrafanaUsers", index=False)
    df_users.to_excel(writer, sheet_name="OrganizationUsers", index=False)
    df_orgs.to_excel(writer, sheet_name="Organizations", index=False)
    df_folders.to_excel(writer, sheet_name="Folders", index=False)
    df_dashboards.to_excel(writer, sheet_name="Dashboards", index=False)
    df_global_summary.to_excel(writer, sheet_name="Summary", index=False)

logger.info(f"Готово! Данные сохранены в файл: {OUTPUT_FILE}")
