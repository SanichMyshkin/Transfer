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

ACTIVITY_FILE = os.getenv("ACTIVITY_FILE", "activity.xlsx")

GRAFANA_REPORT_FILE = os.getenv("GRAFANA_REPORT_FILE", "grafana_report.xlsx")

SLEEP_AFTER_SWITCH = 1
SLEEP_BETWEEN_CALLS = 0.2

INCLUDE_ALL_ZERO_NUMBERS = False

BAN_SERVICE_IDS = [15473]

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


ban_set = build_ban_set(BAN_SERVICE_IDS)


def validate_env_and_files():
    if not GRAFANA_URL:
        die("ENV GRAFANA_URL пустой")
    if not GRAFANA_USER:
        die("ENV GRAFANA_USER пустой")
    if not GRAFANA_PASS:
        die("ENV GRAFANA_PASS пустой")

    if not ACTIVITY_FILE or not os.path.isfile(ACTIVITY_FILE):
        die(f"ACTIVITY_FILE не найден: {ACTIVITY_FILE}")

    logger.info(f"Бан-лист (КОД): {sorted(ban_set) if ban_set else 'пусто'}")
    logger.info(f"INCLUDE_ALL_ZERO_NUMBERS={INCLUDE_ALL_ZERO_NUMBERS}")


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


def is_all_zeros_number(num: str | None) -> bool:
    if not num:
        return False
    return bool(re.fullmatch(r"0+", str(num).strip()))


def load_activity_mapping(path):
    df = pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    map_by_number = {}

    for _, row in df.iterrows():
        num = normalize_number(row.iloc[0] if len(row) > 0 else "")
        service_name = clean_spaces(row.iloc[1] if len(row) > 1 else "")
        activity_code = clean_spaces(row.iloc[2] if len(row) > 2 else "")
        activity_name = clean_spaces(row.iloc[3] if len(row) > 3 else "")

        if not num:
            continue

        if num in map_by_number:
            continue

        map_by_number[num] = {
            "service_name": service_name,
            "activity_code": activity_code,
            "activity_name": activity_name,
        }

    logger.info(f"ACTIVITY: загружено сервисов по номеру={len(map_by_number)}")
    return map_by_number


def compute_panels_for_org(org_id):
    if not switch_org(org_id):
        return None

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

    return panels_total


def main():
    validate_env_and_files()
    logger.info("Старт отчёта Grafana")

    login_cookie()

    activity_map = load_activity_mapping(ACTIVITY_FILE)

    org_ids = sorted(get_unique_org_ids())[:ORG_LIMIT]
    logger.info(f"Организаций к обработке: {len(org_ids)}")

    rows_orgs = []
    unaccounted_orgs = []

    skipped_zero = 0
    skipped_ban = 0
    skipped_activity_miss = 0
    no_access = 0

    def add_unaccounted(
        org_id,
        raw_org_name,
        org_name,
        org_number,
        reason,
        detail,
        service_name="",
        activity_code="",
        activity_name="",
        panels_total=None,
    ):
        rec = {
            "org_id": org_id,
            "org_name_raw": raw_org_name,
            "org_name": org_name,
            "service_id": org_number,
            "service_name": service_name,
            "activity_code": activity_code,
            "activity_name": activity_name,
            "reason": reason,
            "detail": detail,
            "Кол-во панелей": panels_total,
        }
        unaccounted_orgs.append(rec)
        return rec

    for org_id in org_ids:
        raw_org_name = get_org_name(org_id)
        org_name, org_number = split_org_name(raw_org_name)

        norm_number = normalize_number(org_number)
        is_zero = is_all_zeros_number(norm_number)

        service_name = ""
        activity_code = ""
        activity_name = ""

        if norm_number and norm_number in activity_map:
            service_name = activity_map[norm_number]["service_name"]
            activity_code = activity_map[norm_number]["activity_code"]
            activity_name = activity_map[norm_number]["activity_name"]

        if is_zero and not INCLUDE_ALL_ZERO_NUMBERS:
            skipped_zero += 1
            panels_total = compute_panels_for_org(org_id)
            logger.info(
                f'ORG {org_id}: "{org_name}" ({org_number}) — unaccounted (нули) | '
                f"панелей={panels_total if panels_total is not None else 'NO ACCESS'}"
            )
            add_unaccounted(
                org_id,
                raw_org_name,
                org_name,
                org_number,
                reason="skip_zero_number",
                detail="org number is all zeros and INCLUDE_ALL_ZERO_NUMBERS=False",
                service_name=service_name,
                activity_code=activity_code,
                activity_name=activity_name,
                panels_total=panels_total,
            )
            continue

        if (not is_zero) and norm_number and norm_number in ban_set:
            skipped_ban += 1
            panels_total = compute_panels_for_org(org_id)
            logger.info(
                f'ORG {org_id}: "{org_name}" ({org_number}) — unaccounted (бан) | '
                f"панелей={panels_total if panels_total is not None else 'NO ACCESS'}"
            )
            add_unaccounted(
                org_id,
                raw_org_name,
                org_name,
                org_number,
                reason="banned_service_id",
                detail="org number in BAN_SERVICE_IDS",
                service_name=service_name,
                activity_code=activity_code,
                activity_name=activity_name,
                panels_total=panels_total,
            )
            continue

        if norm_number and not is_zero and not clean_spaces(service_name):
            skipped_activity_miss += 1
            panels_total = compute_panels_for_org(org_id)
            logger.info(
                f'ORG {org_id}: "{org_name}" ({org_number}) — unaccounted (нет в activity) | '
                f"панелей={panels_total if panels_total is not None else 'NO ACCESS'}"
            )
            add_unaccounted(
                org_id,
                raw_org_name,
                org_name,
                org_number,
                reason="activity_mapping_miss",
                detail="no match in activity.xlsx by service_id",
                service_name=service_name,
                activity_code=activity_code,
                activity_name=activity_name,
                panels_total=panels_total,
            )
            continue

        display_service_name = service_name if service_name else org_name

        if not switch_org(org_id):
            no_access += 1
            logger.warning(
                f'ORG {org_id}: "{org_name}" — NO ACCESS | '
                f'сервис="{display_service_name}" | '
                f'код активности="{activity_code or "—"}" | '
                f'активность="{activity_name or "—"}"'
            )
            add_unaccounted(
                org_id,
                raw_org_name,
                org_name,
                org_number,
                reason="no_access",
                detail="switch_org returned 401",
                service_name=service_name,
                activity_code=activity_code,
                activity_name=activity_name,
                panels_total=None,
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
            f'код активности="{activity_code or "—"}" | '
            f'активность="{activity_name or "—"}" | '
            f"панелей={panels_total}"
        )

        rows_orgs.append(
            {
                "Имя сервиса": display_service_name,
                "Код": org_number,
                "Код активности": activity_code,
                "Наименование активности": activity_name,
                "Кол-во панелей": panels_total,
                "Потребление в %": None,
            }
        )

    total_panels = sum(r["Кол-во панелей"] for r in rows_orgs if isinstance(r["Кол-во панелей"], int))

    logger.info(
        f"Итог: панелей={total_panels}, skip_zero={skipped_zero}, "
        f"skip_ban={skipped_ban}, activity_miss={skipped_activity_miss}, "
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