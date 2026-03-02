import os
import sys
import logging
import urllib3
import pandas as pd
from dotenv import load_dotenv
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from jenkins_client import JenkinsGroovyClient

logger = logging.getLogger("jenkins_report")
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(console_handler)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

JENKINS_URL = os.getenv("JENKINS_URL")
USER = os.getenv("USER")
TOKEN = os.getenv("TOKEN")

EXCLUDE_PROJECTS_WITHOUT_TEAM_NUMBER = True

SD_FILE = os.getenv("SD_FILE", "sd.xlsx")
BK_FILE = os.getenv("BK_FILE", "bk_all_users.xlsx")
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "jenkins_report.xlsx")

BAN_SERVICE_IDS = [15473]
BAN_BUSINESS_TYPES = []
SKIP_EMPTY_BUSINESS_TYPE = True

SCRIPT_BUILDS = r"""
import jenkins.model.Jenkins
import groovy.json.JsonOutput
def jobs = Jenkins.instance.getAllItems()
def jobList = []
jobs.each { j ->
    def info = [
        name: j.fullName,
        isFolder: j.class.simpleName.contains("Folder")
    ]
    try {
        if (!info.isFolder && j.metaClass.respondsTo(j, "getBuilds")) {
            def builds = j.getBuilds()
            info.buildCount = (builds != null) ? builds.size() : 0
        } else {
            info.buildCount = 0
        }
    } catch (Exception e) {
        info.buildCount = 0
        info.error = e.message
    }
    jobList << info
}
JsonOutput.toJson([jobs: jobList, total: jobs.size()])
"""

client = JenkinsGroovyClient(JENKINS_URL, USER, TOKEN, is_https=False)


def die(msg: str, code: int = 2):
    logger.error(msg)
    raise SystemExit(code)


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


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
    logger.info("Проверяем ENV и входные файлы...")

    if not JENKINS_URL:
        die("ENV JENKINS_URL пустой")
    if not USER:
        die("ENV USER пустой")
    if not TOKEN:
        die("ENV TOKEN пустой")

    if not SD_FILE or not os.path.isfile(SD_FILE):
        die(f"SD_FILE не найден: {SD_FILE}")
    if not BK_FILE or not os.path.isfile(BK_FILE):
        die(f"BK_FILE не найден: {BK_FILE}")

    out_dir = os.path.dirname(OUTPUT_XLSX)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    logger.info(f"Бан-лист (КОД): {sorted(ban_set) if ban_set else 'пусто'}")
    logger.info(
        f"BAN_BUSINESS_TYPES: {sorted(ban_business_types_set) if ban_business_types_set else 'пусто'}"
    )
    logger.info(
        f"EXCLUDE_PROJECTS_WITHOUT_TEAM_NUMBER={EXCLUDE_PROJECTS_WITHOUT_TEAM_NUMBER}"
    )
    logger.info(f"SKIP_EMPTY_BUSINESS_TYPE={SKIP_EMPTY_BUSINESS_TYPE}")
    logger.info("ENV/файлы ок.")


def get_builds_inventory():
    logger.info("Получаем инвентарь Jenkins (jobs + buildCount)...")
    data = client.run_script(SCRIPT_BUILDS)
    total = data.get("total")
    jobs_len = len(data.get("jobs", []) or [])
    logger.info(f"Jenkins вернул items(total): {total}, jobs в payload: {jobs_len}")
    return data


def split_project_and_team(project_raw: str):
    if not project_raw:
        return "", ""
    parts = project_raw.split("-")
    if len(parts) >= 2 and parts[-1].isdigit():
        return "-".join(parts[:-1]), parts[-1]
    return project_raw, ""


def load_sd_people_map(path: str):
    logger.info("Читаем SD (B=КОД, D=Наименование, H=Владелец)...")

    df = pd.read_excel(path, usecols="B,D,H", dtype=str, engine="openpyxl")
    df.columns = ["service_id", "service_name", "owner"]
    df = df.fillna("")

    df["service_id"] = df["service_id"].astype(str).str.strip()
    df["service_name"] = df["service_name"].astype(str).map(clean_spaces)
    df["owner"] = df["owner"].astype(str).map(clean_spaces)

    df = df[df["service_id"] != ""].copy()
    last = df.drop_duplicates("service_id", keep="last")

    mp = {
        sid: {"service_name": sn, "owner": o}
        for sid, sn, o in zip(
            last["service_id"].tolist(),
            last["service_name"].tolist(),
            last["owner"].tolist(),
        )
    }

    logger.info(f"SD: загружено сервисов по КОД: {len(mp)}")
    return mp


def load_bk_business_type_map(path: str):
    logger.info("Читаем BK (A:B:C=ФИО, AS=Тип бизнеса)...")

    df = pd.read_excel(path, usecols="A:C,AS", dtype=str, engine="openpyxl")
    df = df.fillna("")
    df.columns = ["c1", "c2", "c3", "business_type"]

    def make_fio(r):
        fio = " ".join(
            [clean_spaces(r["c2"]), clean_spaces(r["c1"]), clean_spaces(r["c3"])]
        )
        return clean_spaces(fio)

    df["fio_key"] = df.apply(make_fio, axis=1).map(normalize_name_key)
    df["business_type"] = df["business_type"].astype(str).map(clean_spaces)

    df = df[df["fio_key"] != ""].copy()
    last = df.drop_duplicates("fio_key", keep="last")

    mp = dict(zip(last["fio_key"], last["business_type"]))
    logger.info(f"BK: загружено ФИО->Тип бизнеса: {len(mp)}")
    return mp


def aggregate_builds_by_service(
    data, sd_people_map, bk_type_map, exclude_without_team=True
):
    logger.info("Агрегируем билды по сервису...")

    acc = defaultdict(int)
    unaccounted = []

    def add_unaccounted(
        root_project,
        team_number,
        full_name,
        builds,
        reason,
        detail,
        service_name="",
        owner="",
        business_type="",
    ):
        unaccounted.append(
            {
                "root_project": root_project,
                "team_number": team_number,
                "job_full_name": full_name,
                "buildCount": builds,
                "reason": reason,
                "detail": detail,
                "service_name": service_name,
                "owner": owner,
                "business_type": business_type,
            }
        )

    for j in data.get("jobs", []) or []:
        if j.get("isFolder"):
            continue

        full_name = (j.get("name") or "").strip()
        if not full_name:
            continue

        root = full_name.split("/", 1)[0].strip()
        if not root:
            continue

        proj, team_number = split_project_and_team(root)
        builds = int(j.get("buildCount") or 0)

        if exclude_without_team and not team_number:
            add_unaccounted(
                root_project=proj or root,
                team_number="",
                full_name=full_name,
                builds=builds,
                reason="no_team_number",
                detail="root project name does not end with -<digits> (or exclude_without_team=True)",
            )
            continue

        if team_number and team_number in ban_set:
            add_unaccounted(
                root_project=proj or root,
                team_number=team_number,
                full_name=full_name,
                builds=builds,
                reason="banned_service_id",
                detail="team_number in BAN_SERVICE_IDS",
            )
            continue

        if not team_number:
            # сюда попадём, если exclude_without_team=False, но team_number не найден
            add_unaccounted(
                root_project=proj or root,
                team_number="",
                full_name=full_name,
                builds=builds,
                reason="no_team_number_included",
                detail="team_number is empty but exclude_without_team=False; not attributable to service",
            )
            continue

        acc[team_number] += builds

    candidates = []
    kept_service_ids = set()
    filtered_empty_bt = 0
    filtered_ban_bt = 0
    filtered_sd_miss = 0

    for team_number, builds in acc.items():
        people = sd_people_map.get(team_number)
        service_name = (people or {}).get("service_name", "")
        owner = (people or {}).get("owner", "")
        business_type = bk_type_map.get(normalize_name_key(owner), "") if owner else ""
        business_type = clean_spaces(business_type)

        if not people or (not clean_spaces(service_name) and not clean_spaces(owner)):
            filtered_sd_miss += 1
            add_unaccounted(
                root_project="",
                team_number=team_number,
                full_name="",
                builds=builds,
                reason="sd_mapping_miss",
                detail="no match in SD for this team_number",
                service_name=service_name,
                owner=owner,
                business_type=business_type,
            )

        if SKIP_EMPTY_BUSINESS_TYPE and not business_type:
            filtered_empty_bt += 1
            add_unaccounted(
                root_project="",
                team_number=team_number,
                full_name="",
                builds=builds,
                reason="skip_empty_business_type",
                detail="SKIP_EMPTY_BUSINESS_TYPE=True and business_type is empty",
                service_name=service_name,
                owner=owner,
                business_type=business_type,
            )
            continue

        if ban_business_types_set and business_type in ban_business_types_set:
            filtered_ban_bt += 1
            add_unaccounted(
                root_project="",
                team_number=team_number,
                full_name="",
                builds=builds,
                reason="banned_business_type",
                detail="business_type in BAN_BUSINESS_TYPES",
                service_name=service_name,
                owner=owner,
                business_type=business_type,
            )
            continue

        kept_service_ids.add(team_number)
        candidates.append(
            {
                "business_type": business_type,
                "service_name": service_name,
                "team_number": team_number,
                "owner": owner,
                "builds": builds,
            }
        )

    eligible_total_builds = sum(x["builds"] for x in candidates)

    rows = []
    for x in sorted(candidates, key=lambda d: d["builds"], reverse=True):
        builds = x["builds"]
        pct = (
            (builds / eligible_total_builds * 100.0)
            if eligible_total_builds > 0
            else 0.0
        )
        rows.append(
            [
                x["business_type"],
                x["service_name"],
                x["team_number"],
                x["owner"],
                builds,
                round(pct, 2),
            ]
        )

    logger.info(
        f"Фильтры: sd_miss={filtered_sd_miss}, empty_bt={filtered_empty_bt}, ban_bt={filtered_ban_bt}. "
        f"Итого учтенных сервисов={len(candidates)}"
    )

    return rows, unaccounted


def export_excel(rows, unaccounted_rows, filename):
    logger.info("Формируем Excel...")

    wb = Workbook()

    ws = wb.active
    ws.title = "Отчет Jenkins"

    headers = [
        "Тип бизнеса",
        "Наименование сервиса",
        "КОД",
        "Владелец сервиса",
        "Кол-во билдов",
        "% от общего кол-ва билдов",
    ]
    ws.append(headers)

    bold = Font(bold=True)
    for cell in ws[1]:
        cell.font = bold

    for r in rows:
        ws.append(r)

    for col_idx in range(1, len(headers) + 1):
        max_len = 0
        for row in ws.iter_rows(min_col=col_idx, max_col=col_idx, values_only=True):
            v = row[0]
            if v is None:
                continue
            max_len = max(max_len, len(str(v)))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(
            max(12, max_len + 2), 60
        )

    ws2 = wb.create_sheet("Unaccounted")
    headers2 = [
        "root_project",
        "team_number",
        "job_full_name",
        "buildCount",
        "reason",
        "detail",
        "service_name",
        "owner",
        "business_type",
    ]
    ws2.append(headers2)
    for cell in ws2[1]:
        cell.font = bold

    for r in unaccounted_rows:
        ws2.append(
            [
                r.get("root_project", ""),
                r.get("team_number", ""),
                r.get("job_full_name", ""),
                int(r.get("buildCount") or 0),
                r.get("reason", ""),
                r.get("detail", ""),
                r.get("service_name", ""),
                r.get("owner", ""),
                r.get("business_type", ""),
            ]
        )

    for col_idx in range(1, len(headers2) + 1):
        max_len = 0
        for row in ws2.iter_rows(min_col=col_idx, max_col=col_idx, values_only=True):
            v = row[0]
            if v is None:
                continue
            max_len = max(max_len, len(str(v)))
        ws2.column_dimensions[get_column_letter(col_idx)].width = min(
            max(12, max_len + 2), 80
        )

    wb.save(filename)
    logger.info(f"Excel сохранён: {filename}")


def main():
    try:
        validate_env_and_files()

        sd_people_map = load_sd_people_map(SD_FILE)
        bk_type_map = load_bk_business_type_map(BK_FILE)

        data = get_builds_inventory()

        rows, unaccounted = aggregate_builds_by_service(
            data,
            sd_people_map=sd_people_map,
            bk_type_map=bk_type_map,
            exclude_without_team=EXCLUDE_PROJECTS_WITHOUT_TEAM_NUMBER,
        )

        export_excel(rows, unaccounted, OUTPUT_XLSX)

        logger.info(
            f"Инвентаризация билдов завершена успешно. accounted={len(rows)} unaccounted={len(unaccounted)}"
        )
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
