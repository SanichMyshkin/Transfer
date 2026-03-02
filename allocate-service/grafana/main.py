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
ORG_LIMIT = int(os.getenv("ORG_LIMIT", "555"))

SD_FILE = os.getenv("SD_FILE", "sd.xlsx")
BK_FILE = os.getenv("BK_FILE", "bk_all_users.xlsx")

GRAFANA_REPORT_FILE = os.getenv("GRAFANA_REPORT_FILE", "grafana_report.xlsx")

SLEEP_AFTER_SWITCH = 1
SLEEP_BETWEEN_CALLS = 0.2

INCLUDE_ALL_ZERO_NUMBERS = False

BAN_SERVICE_IDS = [15473]

BAN_BUSINESS_TYPES = []

SKIP_EMPTY_BUSINESS_TYPE = True

logger = logging.getLogger("grafana_usage_report")
logger.setLevel(logging.INFO)
fmt = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
)
handler = logging.StreamHandler()
handler.setFormatter(fmt)
logger.handlers.clear()
logger.addHandler(handler)

requests.packages.urllib3.disable_warnings()
session = requests.Session()
session.verify = False


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def die(msg: str, code: int = 2):
    logger.error(msg)
    raise SystemExit(code)


def build_ban_set(ban_list):
    if not isinstance(ban_list, (list, tuple, set)):
        die("BAN_SERVICE_IDS должен быть list / tuple / set")
    return {str(x).strip() for x in ban_list if str(x).strip()}


def build_ban_business_types_set(ban_list):
    if not isinstance(ban_list, (list, tuple, set)):
        die("BAN_BUSINESS_TYPES должен быть list / tuple / set")
    return {clean_spaces(x) for x in ban_list if clean_spaces(x)}


ban_set = build_ban_set(BAN_SERVICE_IDS)
ban_business_types_set = build_ban_business_types_set(BAN_BUSINESS_TYPES)


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
    logger.info(f"INCLUDE_ALL_ZERO_NUMBERS={INCLUDE_ALL_ZERO_NUMBERS}")


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


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


def get_dashboards_in_folder(folder_uid: str):
    out = []
    page = 1
    while True:
        r = session.get(
            f"{GRAFANA_URL}/api/search",
            params={
                "folderUIDs": folder_uid,
                "type": "dash-db",
                "limit": 5000,
                "page": page,
            },
        )
        r.raise_for_status()
        time.sleep(SLEEP_BETWEEN_CALLS)

        batch = r.json() or []
        if not batch:
            break

        out.extend(batch)
        page += 1

    return out


def get_all_dashboards(page: int):
    r = session.get(
        f"{GRAFANA_URL}/api/search",
        params={"type": "dash-db", "limit": 5000, "page": page},
    )
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)
    return r.json() or []


def get_root_dashboards():
    out = []
    page = 1
    while True:
        batch = get_all_dashboards(page)
        if not batch:
            break
        out.extend([d for d in batch if d.get("folderId") in (0, None)])
        page += 1
    return out


def get_dashboard_panels(uid):
    r = session.get(f"{GRAFANA_URL}/api/dashboards/uid/{uid}")
    if r.status_code in (401, 404):
        return 0
    r.raise_for_status()
    time.sleep(SLEEP_BETWEEN_CALLS)

    dash = r.json().get("dashboard", {}) or {}

    def walk(panels) -> int:
        if not panels:
            return 0
        cnt = 0
        for p in panels:
            if not isinstance(p, dict):
                continue

            p_type = (p.get("type") or "").lower()

            if p_type == "row":
                cnt += walk(p.get("panels"))
                continue

            cnt += 1
            cnt += walk(p.get("panels"))
        return cnt

    return walk(dash.get("panels"))


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
    return bool(re.fullmatch(r"0+", str(num).strip()))


def load_bk_business_type_map(path: str):
    df = pd.read_excel(path, usecols="A:C,AS", dtype=str, engine="openpyxl").fillna("")
    df.columns = ["c1", "c2", "c3", "business_type"]

    def make_fio(r):
        fio = " ".join(
            [clean_spaces(r["c2"]), clean_spaces(r["c1"]), clean_spaces(r["c3"])]
        )
        return clean_spaces(fio)

    df["fio_key"] = df.apply(make_fio, axis=1).map(normalize_name_key)
    df["business_type"] = df["business_type"].astype(str).map(clean_spaces)

    last = df[df["fio_key"] != ""].drop_duplicates("fio_key", keep="last")
    mp = dict(zip(last["fio_key"], last["business_type"]))

    logger.info(f"BK: загружено ФИО → тип бизнеса: {len(mp)}")
    return mp


def load_sd_mapping(path):
    df = pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    map_by_number = {}
    map_by_name = {}

    for _, row in df.iterrows():
        num = normalize_number(row.iloc[1])
        sd_name_raw = row.iloc[3]
        owner_raw = row.iloc[7] if len(row) > 7 else ""

        sd_name = clean_spaces(sd_name_raw)
        owner = clean_spaces(owner_raw)

        payload = {"sd_name": sd_name, "owner": owner}

        if num:
            map_by_number[num] = payload
        if sd_name:
            map_by_name[normalize_name(sd_name)] = payload

    logger.info(
        f"SD: загружено сервисов по номеру={len(map_by_number)}, по имени={len(map_by_name)}"
    )
    return map_by_number, map_by_name


def main():
    validate_env_and_files()
    logger.info("Старт отчёта Grafana")

    login_cookie()

    bk_type_map = load_bk_business_type_map(BK_FILE)
    map_by_number, map_by_name = load_sd_mapping(SD_FILE)

    org_ids = sorted(get_unique_org_ids())[:ORG_LIMIT]
    logger.info(f"Организаций к обработке: {len(org_ids)}")

    rows_orgs = []
    unaccounted_orgs = []

    skipped_zero = skipped_ban = no_access = 0
    skipped_empty_bt = skipped_ban_bt = skipped_sd_miss = 0

    def add_unaccounted(
        org_id,
        raw_org_name,
        org_name,
        org_number,
        reason,
        detail,
        sd_name="",
        owner="",
        business_type="",
    ):
        unaccounted_orgs.append(
            {
                "org_id": org_id,
                "org_name_raw": raw_org_name,
                "org_name": org_name,
                "org_number": org_number,
                "reason": reason,
                "detail": detail,
                "sd_name": sd_name,
                "owner": owner,
                "business_type": business_type,
            }
        )

    for org_id in org_ids:
        raw_org_name = get_org_name(org_id)
        org_name, org_number = split_org_name(raw_org_name)

        norm_number = normalize_number(org_number)
        norm_name = normalize_name(org_name)

        is_zero = is_all_zeros_number(norm_number)

        if is_zero and not INCLUDE_ALL_ZERO_NUMBERS:
            skipped_zero += 1
            logger.info(f'ORG {org_id}: "{org_name}" ({org_number}) — skip (нули)')
            add_unaccounted(
                org_id,
                raw_org_name,
                org_name,
                org_number,
                reason="skip_zero_number",
                detail="org number is all zeros and INCLUDE_ALL_ZERO_NUMBERS=False",
            )
            continue

        if (not is_zero) and norm_number and norm_number in ban_set:
            skipped_ban += 1
            logger.info(f'ORG {org_id}: "{org_name}" ({org_number}) — skip (бан)')
            add_unaccounted(
                org_id,
                raw_org_name,
                org_name,
                org_number,
                reason="banned_service_id",
                detail="org number in BAN_SERVICE_IDS",
            )
            continue

        sd_name = ""
        owner = ""
        business_type = ""

        if not is_zero:
            sd = None
            if norm_number and norm_number in map_by_number:
                sd = map_by_number[norm_number]
            elif norm_name and norm_name in map_by_name:
                sd = map_by_name[norm_name]

            sd_name = (sd or {}).get("sd_name") or ""
            owner = (sd or {}).get("owner") or ""
            business_type = (
                bk_type_map.get(normalize_name_key(owner), "") if owner else ""
            )

            if not sd_name and not owner:
                skipped_sd_miss += 1
                add_unaccounted(
                    org_id,
                    raw_org_name,
                    org_name,
                    org_number,
                    reason="sd_mapping_miss",
                    detail="no match in SD by number or by name",
                    sd_name=sd_name,
                    owner=owner,
                    business_type=business_type,
                )

            if SKIP_EMPTY_BUSINESS_TYPE and not clean_spaces(business_type):
                skipped_empty_bt += 1
                add_unaccounted(
                    org_id,
                    raw_org_name,
                    org_name,
                    org_number,
                    reason="skip_empty_business_type",
                    detail="SKIP_EMPTY_BUSINESS_TYPE=True and business_type is empty",
                    sd_name=sd_name,
                    owner=owner,
                    business_type=business_type,
                )
                continue

            if (
                ban_business_types_set
                and clean_spaces(business_type) in ban_business_types_set
            ):
                skipped_ban_bt += 1
                add_unaccounted(
                    org_id,
                    raw_org_name,
                    org_name,
                    org_number,
                    reason="banned_business_type",
                    detail="business_type in BAN_BUSINESS_TYPES",
                    sd_name=sd_name,
                    owner=owner,
                    business_type=business_type,
                )
                continue

        display_service_name = sd_name if sd_name else org_name

        if not switch_org(org_id):
            no_access += 1
            logger.warning(
                f'ORG {org_id}: "{org_name}" — NO ACCESS | '
                f'сервис="{display_service_name}" | '
                f'владелец="{owner or "—"}" | '
                f'тип="{business_type or "—"}"'
            )
            add_unaccounted(
                org_id,
                raw_org_name,
                org_name,
                org_number,
                reason="no_access",
                detail="switch_org returned 401",
                sd_name=sd_name,
                owner=owner,
                business_type=business_type,
            )
            continue

        panels_total = 0

        folders = get_folders()
        for f in folders:
            folder_uid = f.get("uid")
            if not folder_uid:
                continue
            dashboards = get_dashboards_in_folder(folder_uid)
            for d in dashboards:
                panels_total += get_dashboard_panels(d["uid"])

        for d in get_root_dashboards():
            panels_total += get_dashboard_panels(d["uid"])

        logger.info(
            f'ORG {org_id}: "{org_name}" ({org_number}) | '
            f'сервис="{display_service_name}" | '
            f'владелец="{owner or "—"}" | '
            f'тип="{business_type or "—"}" | '
            f"панелей={panels_total}"
        )

        rows_orgs.append(
            {
                "Тип бизнеса": business_type,
                "Наименование сервиса": display_service_name,
                "КОД": org_number,
                "Владелец сервиса": owner,
                "Кол-во панелей": panels_total,
                "Потребление в %": None,
            }
        )

    total_panels = sum(
        r["Кол-во панелей"] for r in rows_orgs if isinstance(r["Кол-во панелей"], int)
    )

    logger.info(
        f"Итог: панелей={total_panels}, skip_zero={skipped_zero}, "
        f"skip_ban={skipped_ban}, sd_miss={skipped_sd_miss}, "
        f"skip_empty_bt={skipped_empty_bt}, ban_bt={skipped_ban_bt}, "
        f"no_access={no_access}"
    )

    for r in rows_orgs:
        if isinstance(r["Кол-во панелей"], int) and total_panels > 0:
            r["Потребление в %"] = round(r["Кол-во панелей"] / total_panels * 100, 2)

    df = pd.DataFrame(rows_orgs)
    df_un = pd.DataFrame(unaccounted_orgs)

    with pd.ExcelWriter(GRAFANA_REPORT_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Отчет Grafana", index=False)
        df_un.to_excel(writer, sheet_name="Unaccounted orgs", index=False)

    logger.info(
        f"Готово. Файл сохранён: {GRAFANA_REPORT_FILE} | "
        f"accounted={len(rows_orgs)} unaccounted={len(unaccounted_orgs)}"
    )


if __name__ == "__main__":
    main()
