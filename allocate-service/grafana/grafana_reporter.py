import os
import time
import logging
import requests
import pandas as pd

from dotenv import load_dotenv
from tqdm import tqdm
from gitlab_config_loader import get_unique_org_ids

load_dotenv()

GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")

OUTPUT_FILE = os.getenv("OUTPUT_FILE", "grafana_usage_report.xlsx")
ORG_LIMIT = int(os.getenv("ORG_LIMIT", "100"))

SLEEP_AFTER_SWITCH = 1
SLEEP_BETWEEN_CALLS = 0.2

logger = logging.getLogger("grafana_usage_report")
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


def main():
    login_cookie()

    org_ids = sorted(get_unique_org_ids())[:ORG_LIMIT]

    rows_orgs = []

    for org_id in tqdm(org_ids, desc="Организации"):
        org_name = get_org_name(org_id)

        if not switch_org(org_id):
            rows_orgs.append(
                {
                    "organization": org_name,
                    "org_id": org_id,
                    "dashboards_total": "NO ACCESS",
                    "panels_total": "NO ACCESS",
                    "usage_percent": None,
                }
            )
            continue

        folders = get_folders()
        dashboards_total = 0
        panels_total = 0

        for f in folders:
            dashboards = get_dashboards_in_folder(f["id"])
            dashboards_total += len(dashboards)

            for d in dashboards:
                panels = get_dashboard_panels(d["uid"])
                panels_total += panels

        for d in get_root_dashboards():
            panels = get_dashboard_panels(d["uid"])
            panels_total += panels
            dashboards_total += 1

        rows_orgs.append(
            {
                "organization": org_name,
                "org_id": org_id,
                "dashboards_total": dashboards_total,
                "panels_total": panels_total,
                "usage_percent": None,
            }
        )

    total_resources = 0
    for row in rows_orgs:
        d = row["dashboards_total"]
        p = row["panels_total"]
        if isinstance(d, int) and isinstance(p, int):
            total_resources += d + p

    for row in rows_orgs:
        d = row["dashboards_total"]
        p = row["panels_total"]
        if isinstance(d, int) and isinstance(p, int) and total_resources > 0:
            resources = d + p
            row["usage_percent"] = round(resources / total_resources * 100, 2)
        else:
            row["usage_percent"] = None

    df = pd.DataFrame(rows_orgs)
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="OrgUsage", index=False)

    logger.info(f"Готово. Файл сохранён: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
