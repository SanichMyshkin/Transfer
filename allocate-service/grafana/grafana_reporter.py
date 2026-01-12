import os
import time
import logging
import requests
import pandas as pd
import re
from dotenv import load_dotenv
from tqdm import tqdm
from gitlab_config_loader import get_unique_org_ids

load_dotenv()

GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")
ORG_LIMIT = int(os.getenv("ORG_LIMIT", "5"))
BUSINESS_FILE = os.getenv("BUSINESS_FILE", "business.xlsx")

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


def split_org_name(raw):
    s = raw.strip()
    m = re.search(r"-\s*(\d+)$", s)
    if m:
        name = s[: m.start()].strip()
        number = m.group(1)
        return name, number
    if "-" in s:
        name, number = s.rsplit("-", 1)
        return name.strip(), number.strip()
    return s, ""


def normalize_number(x):
    """Приводим номер к строке для сопоставления (7704, '7704.0', '7704' -> '7704')."""
    if pd.isna(x):
        return None
    s = str(x).strip()
    # если формат типа 7704.0
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".", 1)[0]
    return s


def load_business_mapping(path: str) -> dict:
    """
    Читаем business.xlsx и строим мапу:
    колонка B (индекс 1) -> колонка E (индекс 4).
    """
    if not os.path.exists(path):
        logger.warning(f"Файл {path} не найден, тип бизнеса подставлен не будет")
        return {}

    df_b = pd.read_excel(path)
    mapping = {}

    for _, row in df_b.iterrows():
        num = normalize_number(row.iloc[1])   # колонка B
        biz_type = row.iloc[4]               # колонка E
        if num and not pd.isna(biz_type):
            mapping[num] = str(biz_type).strip()

    return mapping


def main():
    login_cookie()

    # мапа номер -> тип бизнеса из business.xlsx
    business_map = load_business_mapping(BUSINESS_FILE)

    org_ids = sorted(get_unique_org_ids())[:ORG_LIMIT]
    rows_orgs = []

    for org_id in tqdm(org_ids, desc="Организации"):
        raw_org_name = get_org_name(org_id)
        org_name, org_number = split_org_name(raw_org_name)
        norm_number = normalize_number(org_number)
        biz_type = business_map.get(norm_number)

        if not switch_org(org_id):
            rows_orgs.append(
                {
                    "Тип бизнеса": biz_type,
                    "Наименование сервиса": org_name,
                    "Номер": org_number if org_number else "NO ACCESS",
                    "Кол-во дашбордов": "NO ACCESS",
                    "Кол-во панелей": "NO ACCESS",
                    "Потребление в %": None,
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
                "Тип бизнеса": biz_type,
                "Наименование сервиса": org_name,
                "Номер": org_number,
                "Кол-во дашбордов": dashboards_total,
                "Кол-во панелей": panels_total,
                "Потребление в %": None,
            }
        )

    # считаем общий объём только по панелям
    total_panels = 0
    for row in rows_orgs:
        p = row["Кол-во панелей"]
        if isinstance(p, int):
            total_panels += p

    # проценты тоже только по панелям
    for row in rows_orgs:
        p = row["Кол-во панелей"]
        if isinstance(p, int) and total_panels > 0:
            row["Потребление в %"] = round(p / total_panels * 100, 2)
        else:
            row["Потребление в %"] = None

    df = pd.DataFrame(rows_orgs)

    with pd.ExcelWriter("grafana_report.xlsx", engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Отчет", index=False)

    logger.info("Готово. Файл сохранён: grafana_report.xlsx")


if __name__ == "__main__":
    main()
