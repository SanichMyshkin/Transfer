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
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
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

    logger.info("ENV/файлы ок.")


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


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
    logger.info("Читаем SD (B=КОД, D=Наименование, H=Владелец, I=Менеджер)...")

    # B = service_id, D = service_name, H = owner, I = manager
    df = pd.read_excel(path, usecols="B,D,H,I", dtype=str, engine="openpyxl")
    df.columns = ["service_id", "service_name", "owner", "manager"]
    df = df.fillna("")

    df["service_id"] = df["service_id"].astype(str).str.strip()
    df["service_name"] = df["service_name"].astype(str).map(clean_spaces)
    df["owner"] = df["owner"].astype(str).map(clean_spaces)
    df["manager"] = df["manager"].astype(str).map(clean_spaces)

    df = df[df["service_id"] != ""].copy()
    last = df.drop_duplicates("service_id", keep="last")

    mp = {
        sid: {"service_name": sn, "owner": o, "manager": m}
        for sid, sn, o, m in zip(
            last["service_id"].tolist(),
            last["service_name"].tolist(),
            last["owner"].tolist(),
            last["manager"].tolist(),
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
        # порядок оставляю как у тебя: c2 c1 c3
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


def aggregate_builds_by_service(data, sd_people_map, bk_type_map, exclude_without_team=True):
    logger.info("Агрегируем билды по сервису...")

    acc = defaultdict(int)

    skipped_folder = 0
    skipped_empty_name = 0
    skipped_empty_root = 0
    skipped_no_team = 0

    total_jobs_seen = 0
    total_builds_seen = 0

    for j in data.get("jobs", []) or []:
        total_jobs_seen += 1

        if j.get("isFolder"):
            skipped_folder += 1
            continue

        full_name = (j.get("name") or "").strip()
        if not full_name:
            skipped_empty_name += 1
            continue

        root = full_name.split("/", 1)[0].strip()
        if not root:
            skipped_empty_root += 1
            continue

        project_name, team_number = split_project_and_team(root)

        if exclude_without_team and not team_number:
            skipped_no_team += 1
            continue

        builds = int(j.get("buildCount") or 0)
        total_builds_seen += builds

        # ключим строго по КОДу (team_number)
        acc[team_number] += builds

    total_builds = sum(acc.values())
    logger.info(
        "Статистика джоб: всего=%d, папки=%d, пустое_имя=%d, пустой_root=%d, без_кода=%d",
        total_jobs_seen,
        skipped_folder,
        skipped_empty_name,
        skipped_empty_root,
        skipped_no_team,
    )
    logger.info(f"Билдов по учтённым сервисам: {total_builds} (buildCount sum), buildCount total seen: {total_builds_seen}")
    logger.info(f"Уникальных сервисов (по КОД) в отчёте: {len(acc)}")

    rows = []
    for team_number, builds in sorted(acc.items(), key=lambda kv: kv[1], reverse=True):
        people = sd_people_map.get(team_number, {"service_name": "", "owner": "", "manager": ""})
        sd_service_name = people.get("service_name", "")
        owner = people.get("owner", "")
        manager = people.get("manager", "")

        owner_for_report = owner or manager
        business_type = pick_business_type(bk_type_map, owner=owner, manager=manager)

        # Наименование сервиса строго из SD.D, если нет — оставим пустым (как ты хотел)
        service_name = sd_service_name

        pct = (builds / total_builds * 100.0) if total_builds > 0 else 0.0

        rows.append(
            [
                service_name,
                team_number,
                owner_for_report,
                business_type,
                builds,
                round(pct, 2),
            ]
        )

    return rows


def export_excel(rows, filename):
    logger.info("Формируем Excel...")

    wb = Workbook()
    ws = wb.active
    ws.title = "Отчет Jenkins"

    headers = [
        "Наименование сервиса",
        "КОД",
        "Владелец сервиса",
        "Тип бизнеса",
        "Кол-во билдов",
        "% от общего кол-ва билдов",
    ]
    ws.append(headers)

    bold = Font(bold=True)
    for cell in ws[1]:
        cell.font = bold

    for r in rows:
        ws.append(r)

    # авто-ширина по фактическим значениям
    for col_idx in range(1, len(headers) + 1):
        max_len = 0
        for row in ws.iter_rows(min_col=col_idx, max_col=col_idx, values_only=True):
            v = row[0]
            if v is None:
                continue
            max_len = max(max_len, len(str(v)))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max(12, max_len + 2), 60)

    wb.save(filename)
    logger.info(f"Excel сохранён: {filename}")


def main():
    try:
        validate_env_and_files()

        sd_people_map = load_sd_people_map(SD_FILE)
        bk_type_map = load_bk_business_type_map(BK_FILE)

        data = get_builds_inventory()

        rows = aggregate_builds_by_service(
            data,
            sd_people_map=sd_people_map,
            bk_type_map=bk_type_map,
            exclude_without_team=EXCLUDE_PROJECTS_WITHOUT_TEAM_NUMBER,
        )

        export_excel(rows, OUTPUT_XLSX)

        logger.info("Инвентаризация билдов завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
