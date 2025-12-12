import os
import time
import logging
import sqlite3
import requests
import pandas as pd

from dotenv import load_dotenv
from tqdm import tqdm
from gitlab_config_loader import GitLabConfigLoader
from account_classifier import classify_unmatched_users

load_dotenv()

GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")
BK_SQLITE_PATH = os.getenv("BK_SQLITE_PATH")
ALLOWED_DOMAINS = [
    d.strip() for d in os.getenv("ALLOWED_DOMAINS", "").split(",") if d.strip()
]
OUTPUT_FILE = "grafana_report.xlsx"
ORG_LIMIT = int(os.getenv("ORG_LIMIT", "100"))

SLEEP_AFTER_SWITCH = 1
SLEEP_BETWEEN_CALLS = 0.2

logger = logging.getLogger("grafana_report")
logger.setLevel(logging.INFO)
fmt = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
)
handler = logging.StreamHandler()
handler.setFormatter(fmt)
logger.addHandler(handler)

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


def get_all_dashboards():
    r = session.get(
        f"{GRAFANA_URL}/api/search",
        params={"type": "dash-db", "limit": 5000},
    )
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json()


def get_root_dashboards():
    return [d for d in get_all_dashboards() if d.get("folderId") in (0, None)]


def get_dashboard_panels(uid):
    r = session.get(f"{GRAFANA_URL}/api/dashboards/uid/{uid}")
    if r.status_code in (401, 404):
        return 0
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    dash = r.json().get("dashboard", {})
    count = 0
    if "panels" in dash:
        count += len(dash["panels"])
    if "rows" in dash:
        for row in dash["rows"]:
            count += len(row.get("panels", []))
    return count


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


def get_bk_users():
    conn = sqlite3.connect(BK_SQLITE_PATH)
    with conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("select * from bk").fetchall()
    return [dict(r) for r in rows]


def match_users(grafana_users, bk_users):
    bk_by_login = {(u.get("sAMAccountName") or "").lower(): u for u in bk_users}
    bk_by_email = {(u.get("Email") or "").lower(): u for u in bk_users}
    matched = []
    unmatched = []
    for u in grafana_users:
        login = (u.get("login") or "").lower()
        email = (u.get("email") or "").lower()
        found = bk_by_login.get(login) or bk_by_email.get(email)
        if found:
            matched.append(found)
        else:
            unmatched.append(u)
    return matched, unmatched


def main():
    login_cookie()

    loader = GitLabConfigLoader()
    owners_clean = loader.get_owners_clean()
    org_ids = sorted(owners_clean.keys())[:ORG_LIMIT]

    rows_all_users = []
    for u in get_all_grafana_users():
        rows_all_users.append(
            {
                "id": u.get("id"),
                "name": u.get("name"),
                "login": u.get("login"),
                "email": u.get("email"),
                "isAdmin": u.get("isGrafanaAdmin"),
                "lastSeenAt": u.get("lastSeenAt"),
                "isDisabled": u.get("isDisabled"),
            }
        )

    bk_users = get_bk_users()
    matched, unmatched = match_users(rows_all_users, bk_users)

    tech_accounts, terminated_users = classify_unmatched_users(
        unmatched, ALLOWED_DOMAINS
    )

    rows_orgs = []
    rows_users = []
    rows_folders = []
    rows_dashboards = []

    for org_id in tqdm(org_ids, desc="Организации"):
        org_name = get_org_name(org_id)

        if not switch_org(org_id):
            rows_orgs.append(
                {
                    "organization": org_name,
                    "users_total": "NO ACCESS",
                    "folders_total": "NO ACCESS",
                    "dashboards_total": "NO ACCESS",
                    "panels_total": "NO ACCESS",
                }
            )
            continue

        users = get_users_in_org(org_id)
        if users is None:
            continue

        for u in users:
            rows_users.append(
                {
                    "organization": org_name,
                    "login": u.get("login"),
                    "email": u.get("email"),
                    "role": u.get("role"),
                }
            )

        folders = get_folders()
        dashboards_total = 0
        panels_total = 0

        for f in folders:
            dashboards = get_dashboards_in_folder(f["id"])
            dashboards_total += len(dashboards)

            rows_folders.append(
                {
                    "organization": org_name,
                    "folder_id": f["id"],
                    "folder_title": f["title"],
                    "dashboards_count": len(dashboards),
                }
            )

            for d in dashboards:
                panels = get_dashboard_panels(d["uid"])
                panels_total += panels
                rows_dashboards.append(
                    {
                        "organization": org_name,
                        "dashboard_uid": d["uid"],
                        "dashboard_title": d["title"],
                        "panels": panels,
                    }
                )

        for d in get_root_dashboards():
            panels = get_dashboard_panels(d["uid"])
            panels_total += panels
            dashboards_total += 1
            rows_dashboards.append(
                {
                    "organization": org_name,
                    "dashboard_uid": d["uid"],
                    "dashboard_title": d["title"],
                    "panels": panels,
                }
            )

        rows_orgs.append(
            {
                "organization": org_name,
                "users_total": len(users),
                "folders_total": len(folders),
                "dashboards_total": dashboards_total,
                "panels_total": panels_total,
            }
        )

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        pd.DataFrame(rows_all_users).to_excel(
            writer, sheet_name="GrafanaUsers", index=False
        )
        pd.DataFrame(matched).to_excel(writer, sheet_name="BK_Users", index=False)
        pd.DataFrame(tech_accounts).to_excel(
            writer, sheet_name="Tech_Accounts", index=False
        )
        pd.DataFrame(terminated_users).to_excel(
            writer, sheet_name="Terminated_Users", index=False
        )
        pd.DataFrame(rows_users).to_excel(
            writer, sheet_name="OrganizationUsers", index=False
        )
        pd.DataFrame(rows_orgs).to_excel(
            writer, sheet_name="Organizations", index=False
        )
        pd.DataFrame(rows_folders).to_excel(writer, sheet_name="Folders", index=False)
        pd.DataFrame(rows_dashboards).to_excel(
            writer, sheet_name="Dashboards", index=False
        )

    logger.info(f"Готово. Файл сохранён: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
