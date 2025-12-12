import os
import time
import base64
import requests
import logging
from urllib.parse import urlparse
from tqdm import tqdm

try:
    import tomllib
except ImportError:
    import tomli as tomllib

import gitlab

GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GITLAB_PROJECT_ID = os.getenv("GITLAB_PROJECT_ID", "3058")
GITLAB_FILE_PATH = os.getenv("GITLAB_FILE_PATH", "grafana_main/ldap.toml")
GITLAB_REF = os.getenv("GITLAB_REF", "main")

SLEEP = 0.2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("grafana_zabbix_full")

requests.packages.urllib3.disable_warnings()
session = requests.Session()
session.verify = False


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


def dashboard_uses_zabbix(dashboard_json: dict) -> bool:
    panels = dashboard_json.get("dashboard", {}).get("panels", [])

    for panel in panels:
        if not isinstance(panel, dict):
            continue

        ds = panel.get("datasource")

        if isinstance(ds, str) and "zabbix" in ds.lower():
            return True

        if isinstance(ds, dict) and "zabbix" in (ds.get("type") or "").lower():
            return True

        targets = panel.get("targets")
        if not isinstance(targets, list):
            continue

        for target in targets:
            if not isinstance(target, dict):
                continue

            tds = target.get("datasource")

            if isinstance(tds, str) and "zabbix" in tds.lower():
                return True

            if isinstance(tds, dict) and "zabbix" in (tds.get("type") or "").lower():
                return True

    return False


def extract_zabbix_hosts(panel: dict):
    hosts = set()
    groups = set()

    targets = panel.get("targets")
    if not isinstance(targets, list):
        return hosts, groups

    for t in targets:
        if not isinstance(t, dict):
            continue

        h = t.get("host")
        g = t.get("group")

        if isinstance(h, str):
            hosts.add(h)
        elif isinstance(h, list):
            hosts.update(h)

        if isinstance(g, str):
            groups.add(g)
        elif isinstance(g, list):
            groups.update(g)

    return hosts, groups


def extract_zabbix_panels(dashboard_json: dict):
    panels = dashboard_json.get("dashboard", {}).get("panels", [])
    result = []

    for panel in panels:
        if not isinstance(panel, dict):
            continue

        ds = panel.get("datasource")
        is_zabbix = False

        if isinstance(ds, str) and "zabbix" in ds.lower():
            is_zabbix = True
        elif isinstance(ds, dict) and "zabbix" in (ds.get("type") or "").lower():
            is_zabbix = True

        if not is_zabbix:
            continue

        hosts, groups = extract_zabbix_hosts(panel)

        result.append(
            {
                "panel_title": panel.get("title"),
                "hosts": sorted(hosts),
                "groups": sorted(groups),
            }
        )

    return result


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


def main():
    grafana_login()

    org_ids = load_org_ids_from_gitlab()
    logger.info(f"Организаций из Git: {len(org_ids)}")

    rows = []

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

            if not dashboard_uses_zabbix(dash):
                continue

            panels = extract_zabbix_panels(dash)

            for p in panels:
                rows.append(
                    {
                        "organization": org_name,
                        "org_id": org_id,
                        "dashboard_title": title,
                        "dashboard_url": f"{GRAFANA_URL}/d/{uid}",
                        "panel_title": p["panel_title"],
                        "zabbix_hosts": ", ".join(p["hosts"]) if p["hosts"] else "",
                        "zabbix_groups": ", ".join(p["groups"]) if p["groups"] else "",
                    }
                )

    for r in rows:
        print(
            f"{r['organization']} | {r['dashboard_title']} | {r['panel_title']} | "
            f"hosts=[{r['zabbix_hosts']}] groups=[{r['zabbix_groups']}] | {r['dashboard_url']}"
        )

    logger.info(f"Найдено Zabbix-панелей: {len(rows)}")


if __name__ == "__main__":
    main()
