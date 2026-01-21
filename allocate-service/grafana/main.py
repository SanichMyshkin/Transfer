import os
import re
import time
import logging
import unicodedata

import pandas as pd
import requests
from dotenv import load_dotenv

from gitlab_config_loader import get_unique_org_ids

load_dotenv()

GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")
ORG_LIMIT = int(os.getenv("ORG_LIMIT", "5"))
BUSINESS_FILE = os.getenv("BUSINESS_FILE", "buisness.xlsx")

SLEEP_AFTER_SWITCH = 1
SLEEP_BETWEEN_CALLS = 0.2

# 4) Флаг: не учитывать вообще команды, у которых номер состоит только из нулей
EXCLUDE_ALL_ZERO_NUMBERS = True

logger = logging.getLogger("grafana_usage_report")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
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
    logger.info("Успешный логин в Grafana")


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


def split_org_name(raw: str):
    s = raw.strip()
    m = re.search(r"(\d+)\s*$", s)
    if not m:
        return s, ""
    number = m.group(1)
    i = m.start()
    j = i - 1
    while j >= 0:
        ch = s[j]
        if ch.isspace() or unicodedata.category(ch) == "Pd":
            j -= 1
            continue
        break
    name = s[: j + 1].strip()
    return name, number


def normalize_number(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".", 1)[0]
    return s


def normalize_name(x):
    if pd.isna(x):
        return None
    return str(x).strip().casefold()


def is_all_zeros_number(num: str | None) -> bool:
    if not num:
        return False
    s = str(num).strip()
    return bool(re.fullmatch(r"0+", s))


# 1) тип бизнеса убираем; 3) добавляем категорию из столбца C
def load_sd_mapping_with_category(path):
    if not os.path.exists(path):
        logger.warning(f"Файл {path} не найден, категория/имя в SD подставлены не будут")
        return {}, {}

    df_b = pd.read_excel(path)
    map_by_number = {}
    map_by_name = {}

    for _, row in df_b.iterrows():
        num = normalize_number(row.iloc[1])       # B: номер
        category_raw = row.iloc[2]                # C: категория
        sd_name_raw = row.iloc[3]                 # D: имя в SD

        if pd.isna(category_raw) and pd.isna(sd_name_raw):
            continue

        category = None if pd.isna(category_raw) else str(category_raw).strip()
        sd_name = None if pd.isna(sd_name_raw) else str(sd_name_raw).strip()
        key_name = normalize_name(sd_name_raw)

        if num:
            map_by_number[num] = (category, sd_name)
        if key_name:
            map_by_name[key_name] = (category, sd_name)

    logger.info(
        f"Загружено записей из SD-портала: по номеру={len(map_by_number)}, по имени={len(map_by_name)}"
    )
    return map_by_number, map_by_name


def main():
    logger.info("Старт формирования отчёта по использованию Grafana")
    login_cookie()

    map_by_number, map_by_name = load_sd_mapping_with_category(BUSINESS_FILE)
    org_ids = sorted(get_unique_org_ids())[:ORG_LIMIT]
    logger.info(f"Будет обработано организаций: {len(org_ids)}")

    rows_orgs = []

    for org_id in org_ids:
        raw_org_name = get_org_name(org_id)
        org_name, org_number = split_org_name(raw_org_name)

        norm_number = normalize_number(org_number)
        norm_name = normalize_name(org_name)

        # 4) полностью исключаем команды с номером из одних нулей
        if EXCLUDE_ALL_ZERO_NUMBERS and is_all_zeros_number(norm_number):
            logger.info(
                f'Организация {org_id}: "{org_name}" ({org_number}) — пропуск (номер из нулей)'
            )
            continue

        category = None
        sd_name = None
        source = None

        if norm_number and norm_number in map_by_number:
            category, sd_name = map_by_number[norm_number]
            source = "номер"
        elif norm_name and norm_name in map_by_name:
            category, sd_name = map_by_name[norm_name]
            source = "имя"

        if not switch_org(org_id):
            logger.warning(
                f'Нет доступа к организации {org_id}: "{raw_org_name}". '
                f'Категория: {category or "не определена"}, имя в SD: {sd_name or "не найдено"}'
            )
            rows_orgs.append(
                {
                    "Категория": category,
                    "Наименование в SD": sd_name,
                    "Наименование сервиса": org_name,
                    "Номер": "NO ACCESS",
                    "Кол-во панелей": "NO ACCESS",
                    "Потребление в %": None,
                }
            )
            continue

        folders = get_folders()
        panels_total = 0

        # 2) считаем только панели (дашборды не считаем)
        for f in folders:
            dashboards = get_dashboards_in_folder(f["id"])
            for d in dashboards:
                panels_total += get_dashboard_panels(d["uid"])

        for d in get_root_dashboards():
            panels_total += get_dashboard_panels(d["uid"])

        if category or sd_name:
            logger.info(
                f'Организация {org_id}: "{org_name}" ({org_number}) — '
                f"панелей={panels_total}, "
                f'категория="{category}", имя в SD="{sd_name}" (найдено по {source})'
            )
        else:
            logger.info(
                f'Организация {org_id}: "{org_name}" ({org_number}) — '
                f"панелей={panels_total}, категория/имя в SD не найдены"
            )

        rows_orgs.append(
            {
                "Категория": category,
                "Наименование в SD": sd_name,
                "Наименование сервиса": org_name,
                "Номер": org_number,
                "Кол-во панелей": panels_total,
                "Потребление в %": None,
            }
        )

    total_panels = 0
    for row in rows_orgs:
        p = row["Кол-во панелей"]
        if isinstance(p, int):
            total_panels += p

    logger.info(f"Общее количество панелей по всем доступным организациям: {total_panels}")

    for row in rows_orgs:
        p = row["Кол-во панелей"]
        if isinstance(p, int) and total_panels > 0:
            row["Потребление в %"] = round(p / total_panels * 100, 2)
        else:
            row["Потребление в %"] = None

    df = pd.DataFrame(rows_orgs)

    output_file = "grafana_report.xlsx"
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Отчет", index=False)

    logger.info(f"Готово. Файл сохранён: {output_file}")


if __name__ == "__main__":
    main()
