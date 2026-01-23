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

SD_FILE = os.getenv("SD_FILE", "sd.xlsx")
BK_FILE = os.getenv("BK_FILE", "bk_all_users.xlsx")

GRAFANA_REPORT_FILE = os.getenv("GRAFANA_REPORT_FILE", "grafana_report.xlsx")

SLEEP_AFTER_SWITCH = 1
SLEEP_BETWEEN_CALLS = 0.2

EXCLUDE_ALL_ZERO_NUMBERS = True

BAN_SERVICE_IDS = [15473]  # сюда дополняешь коды, которые не учитываем

logger = logging.getLogger("grafana_usage_report")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
handler = logging.StreamHandler()
handler.setFormatter(fmt)
logger.handlers.clear()
logger.addHandler(handler)

requests.packages.urllib3.disable_warnings()
session = requests.Session()
session.verify = False


def die(msg: str, code: int = 2):
    logger.error(msg)
    raise SystemExit(code)


def build_ban_set(ban_list):
    if not isinstance(ban_list, (list, tuple, set)):
        die("BAN_SERVICE_IDS должен быть list / tuple / set")
    return {str(x).strip() for x in ban_list if str(x).strip()}


ban_set = build_ban_set(BAN_SERVICE_IDS)


def validate_env_and_files():
    if not GRAFANA_URL:
        die("ENV GRAFANA_URL пустой")
    if not GRAFANA_USER:
        die("ENV GRAFANA_USER пустой")
    if not GRAFANA_PASS:
        die("ENV GRAFANA_PASS пустой")

    if not SD_FILE or not os.path.isfile(SD_FILE):
        die(f"SD_FILE не найден: {SD_FILE}")
    if not BK_FILE or not os.path.isfile(BK_FILE):
        die(f"BK_FILE не найден: {BK_FILE}")

    logger.info(f"Бан-лист (КОД): {sorted(ban_set) if ban_set else 'пусто'}")


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


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


def normalize_name(x):
    if pd.isna(x):
        return None
    return str(x).strip().casefold()


def is_all_zeros_number(num: str | None) -> bool:
    if not num:
        return False
    s = str(num).strip()
    return bool(re.fullmatch(r"0+", s))


def load_sd_mapping(path):
    # SD: B=номер(код), D=имя сервиса, H=владелец, I=менеджер
    df = pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")

    map_by_number = {}
    map_by_name = {}

    for _, row in df.iterrows():
        num = normalize_number(row.iloc[1])           # B: номер/код
        sd_name_raw = row.iloc[3]                     # D: имя в SD
        owner_raw = row.iloc[7] if len(row) > 7 else ""   # H
        manager_raw = row.iloc[8] if len(row) > 8 else "" # I

        sd_name = clean_spaces(sd_name_raw)
        owner = clean_spaces(owner_raw)
        manager = clean_spaces(manager_raw)

        key_name = normalize_name(sd_name_raw)

        if num:
            map_by_number[num] = {"service_name": sd_name, "owner": owner, "manager": manager}
        if key_name:
            map_by_name[key_name] = {"service_name": sd_name, "owner": owner, "manager": manager}

    logger.info(f"Загружено записей из SD: по номеру={len(map_by_number)}, по имени={len(map_by_name)}")
    return map_by_number, map_by_name


def load_bk_business_type_map(path: str):
    # BK: A,B,C=ФИО частями, AS=тип бизнеса
    df = pd.read_excel(path, usecols="A:C,AS", dtype=str, engine="openpyxl").fillna("")
    df.columns = ["c1", "c2", "c3", "business_type"]

    def make_fio(r):
        fio = " ".join([clean_spaces(r["c2"]), clean_spaces(r["c1"]), clean_spaces(r["c3"])])
        return clean_spaces(fio)

    df["fio_key"] = df.apply(make_fio, axis=1).map(normalize_name_key)
    df["business_type"] = df["business_type"].astype(str).map(clean_spaces)

    df = df[df["fio_key"] != ""].copy()
    last = df.drop_duplicates("fio_key", keep="last")

    mp = dict(zip(last["fio_key"], last["business_type"]))
    logger.info(f"BK: загружено ФИО->Тип бизнеса: {len(mp)}")
    return mp


def pick_business_type(bk_type_map: dict, owner: str, manager: str) -> str:
    if owner:
        bt = bk_type_map.get(normalize_name_key(owner), "")
        if bt:
            return bt
    if manager:
        bt = bk_type_map.get(normalize_name_key(manager), "")
        if bt:
            return bt
    return ""


def main():
    validate_env_and_files()

    logger.info("Старт формирования отчёта по использованию Grafana")
    login_cookie()

    map_by_number, map_by_name = load_sd_mapping(SD_FILE)
    bk_type_map = load_bk_business_type_map(BK_FILE)

    org_ids = sorted(get_unique_org_ids())[:ORG_LIMIT]
    logger.info(f"Будет обработано организаций: {len(org_ids)} (лимит ORG_LIMIT={ORG_LIMIT})")

    rows_orgs = []

    skipped_all_zeros = 0
    skipped_banned = 0
    no_access = 0
    processed = 0

    for org_id in org_ids:
        raw_org_name = get_org_name(org_id)
        org_name, org_number = split_org_name(raw_org_name)

        norm_number = normalize_number(org_number)
        norm_name = normalize_name(org_name)

        if EXCLUDE_ALL_ZERO_NUMBERS and is_all_zeros_number(norm_number):
            skipped_all_zeros += 1
            logger.info(f'Организация {org_id}: "{org_name}" ({org_number}) — пропуск (номер из нулей)')
            continue

        if norm_number and norm_number in ban_set:
            skipped_banned += 1
            logger.info(f'Организация {org_id}: "{org_name}" ({org_number}) — пропуск (в бан-листе)')
            continue

        processed += 1

        sd = None
        source = None

        if norm_number and norm_number in map_by_number:
            sd = map_by_number[norm_number]
            source = "номер"
        elif norm_name and norm_name in map_by_name:
            sd = map_by_name[norm_name]
            source = "имя"

        service_name_sd = (sd or {}).get("service_name") or ""
        owner = (sd or {}).get("owner") or ""
        manager = (sd or {}).get("manager") or ""
        owner_for_report = owner or manager
        business_type = pick_business_type(bk_type_map, owner=owner, manager=manager)

        if not switch_org(org_id):
            no_access += 1
            logger.warning(
                f'Нет доступа к организации {org_id}: "{raw_org_name}". '
                f'SD имя="{service_name_sd or "не найдено"}" (по {source or "—"}), '
                f'владелец="{owner_for_report or "—"}", тип бизнеса="{business_type or "—"}"'
            )
            rows_orgs.append(
                {
                    "Тип бизнеса": business_type,
                    "Владелец сервиса": owner_for_report,
                    "Наименование сервиса": service_name_sd,
                    "КОД": org_number,
                    "Кол-во панелей": "NO ACCESS",
                    "Потребление в %": None,
                }
            )
            continue

        folders = get_folders()
        panels_total = 0

        for f in folders:
            dashboards = get_dashboards_in_folder(f["id"])
            for d in dashboards:
                panels_total += get_dashboard_panels(d["uid"])

        for d in get_root_dashboards():
            panels_total += get_dashboard_panels(d["uid"])

        logger.info(
            f'Организация {org_id}: "{org_name}" ({org_number}) — панелей={panels_total}, '
            f'SD имя="{service_name_sd or "—"}" (по {source or "—"}), '
            f'владелец="{owner_for_report or "—"}", тип бизнеса="{business_type or "—"}"'
        )

        rows_orgs.append(
            {
                "Тип бизнеса": business_type,
                "Владелец сервиса": owner_for_report,
                "Наименование сервиса": service_name_sd,
                "КОД": org_number,
                "Кол-во панелей": panels_total,
                "Потребление в %": None,
            }
        )

    total_panels = 0
    for row in rows_orgs:
        p = row["Кол-во панелей"]
        if isinstance(p, int):
            total_panels += p

    logger.info(
        f"Итоги: обработано={processed}, скип_нули={skipped_all_zeros}, скип_бан={skipped_banned}, no_access={no_access}"
    )
    logger.info(f"Общее количество панелей (только учтённые): {total_panels}")

    for row in rows_orgs:
        p = row["Кол-во панелей"]
        if isinstance(p, int) and total_panels > 0:
            row["Потребление в %"] = round(p / total_panels * 100, 2)
        else:
            row["Потребление в %"] = None

    df = pd.DataFrame(rows_orgs)

    with pd.ExcelWriter(GRAFANA_REPORT_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Отчет Grafana", index=False)

    logger.info(f"Готово. Файл сохранён: {GRAFANA_REPORT_FILE}")


if __name__ == "__main__":
    main()
