import os
import time
import base64
import requests
import logging
from tqdm import tqdm

try:
    import tomllib
except ImportError:
    import tomli as tomllib

import gitlab

# ================= ENV =================

GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GITLAB_PROJECT_ID = os.getenv("GITLAB_PROJECT_ID", "3058")
GITLAB_FILE_PATH = os.getenv("GITLAB_FILE_PATH", "grafana_main/ldap.toml")
GITLAB_REF = os.getenv("GITLAB_REF", "main")

SLEEP = 0.2

# ================= LOGGING =================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("grafana_zabbix")

# ================= HTTP =================

requests.packages.urllib3.disable_warnings()
session = requests.Session()
session.verify = False

# ================= GRAFANA =================


def grafana_login():
    r = session.post(
        f"{GRAFANA_URL}/login",
        json={"user": GRAFANA_USER, "password": GRAFANA_PASS},
    )
    r.raise_for_status()
    time.sleep(SLEEP)


def switch_org(org_id: int) -> bool:
    r = session.post(f"{GRAFANA_URL}/api/user/using/{org_id}")
    if r.status_code == 401:
        return False
    r.raise_for_status()
    time.sleep(SLEEP)
    return True


def get_org_name(org_id: int) -> str:
    r = session.get(f"{GRAFANA_URL}/api/orgs/{org_id}")
    if r.status_code != 200:
        return f"ORG_{org_id}"
    return r.json().get("name") or f"ORG_{org_id}"


def get_all_dashboards():
    r = session.get(
        f"{GRAFANA_URL}/api/search",
        params={"type": "dash-db", "limit": 5000},
    )
    r.raise_for_status()
    time.sleep(SLEEP)
    return r.json()


def get_dashboard(uid: str):
    r = session.get(f"{GRAFANA_URL}/api/dashboards/uid/{uid}")
    if r.status_code != 200:
        return None
    time.sleep(SLEEP)
    return r.json()


# ================= ZABBIX DETECTION =================


def dashboard_uses_zabbix(dashboard_json: dict) -> bool:
    panels = dashboard_json.get("dashboard", {}).get("panels", [])

    for panel in panels:
        ds = panel.get("datasource")

        if isinstance(ds, str):
            if "zabbix" in ds.lower():
                return True

        elif isinstance(ds, dict):
            if "zabbix" in (ds.get("type") or "").lower():
                return True

        for target in panel.get("targets", []):
            tds = target.get("datasource")

            if isinstance(tds, str):
                if "zabbix" in tds.lower():
                    return True

            elif isinstance(tds, dict):
                if "zabbix" in (tds.get("type") or "").lower():
                    return True

    return False


# ================= GITLAB =================


def load_org_ids_from_gitlab():
    gl = gitlab.Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN, ssl_verify=False)
    project = gl.projects.get(GITLAB_PROJECT_ID)

    f = project.files.get(file_path=GITLAB_FILE_PATH, ref=GITLAB_REF)
    raw = base64.b64decode(f.content).decode("utf-8")
    data = tomllib.loads(raw)

    org_ids = set()
    servers = data.get("servers")

    if isinstance(servers, dict):
        servers = [servers]

    for s in servers or []:
        for m in s.get("group_mappings", []):
            if "org_id" in m:
                org_ids.add(m["org_id"])

    return sorted(org_ids)


# ================= MAIN =================


def main():
    grafana_login()

    org_ids = load_org_ids_from_gitlab()
    logger.info(f"Организаций из Git: {len(org_ids)}")

    results = []

    for org_id in tqdm(org_ids, desc="Scan orgs", ncols=100):
        if not switch_org(org_id):
            continue

        org_name = get_org_name(org_id)

        dashboards = get_all_dashboards()

        for d in dashboards:
            uid = d.get("uid")
            title = d.get("title")

            dash = get_dashboard(uid)
            if not dash:
                continue

            if dashboard_uses_zabbix(dash):
                results.append(
                    {
                        "org_id": org_id,
                        "organization": org_name,
                        "dashboard_title": title,
                        "dashboard_url": f"{GRAFANA_URL}/d/{uid}",
                    }
                )

    for r in results:
        print(
            f"[{r['org_id']}] {r['organization']} | {r['dashboard_title']} | {r['dashboard_url']}"
        )

    logger.info(f"Найдено Zabbix-дашбордов: {len(results)}")


if __name__ == "__main__":
    main()
