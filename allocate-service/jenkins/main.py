import os
import sys
import logging
import urllib3
import pandas as pd
from dotenv import load_dotenv
from collections import defaultdict

from openpyxl import Workbook
from openpyxl.styles import Font

from jenkins_client import JenkinsGroovyClient


logger = logging.getLogger()
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


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


def get_builds_inventory():
    logger.info("Получаем список и считаем билды...")
    data = client.run_script(SCRIPT_BUILDS)
    logger.info(f"Найдено items: {data.get('total')}")
    return data


def split_project_and_team(project_raw: str):
    if not project_raw:
        return "", ""
    parts = project_raw.split("-")
    if len(parts) >= 2 and parts[-1].isdigit():
        return "-".join(parts[:-1]), parts[-1]
    return project_raw, ""


def load_sd_people_map(path: str):
    # B = service_id, H = owner, I = manager
    df = pd.read_excel(path, usecols="B,H,I", dtype=str, engine="openpyxl")
    df.columns = ["service_id", "owner", "manager"]
    df = df.fillna("")

    df["service_id"] = df["service_id"].astype(str).str.strip()
    df["owner"] = df["owner"].astype(str).map(clean_spaces)
    df["manager"] = df["manager"].astype(str).map(clean_spaces)

    df = df[df["service_id"] != ""].copy()
    last = df.drop_duplicates("service_id", keep="last")

    return {
        sid: {"owner": o, "manager": m}
        for sid, o, m in zip(
            last["service_id"].tolist(),
            last["owner"].tolist(),
            last["manager"].tolist(),
        )
    }


def load_bk_business_type_map(path: str):
    # A,B,C -> ФИО частями, AS -> тип бизнеса
    df = pd.read_excel(path, usecols="A:C,AS", dtype=str, engine="openpyxl")
    df = df.fillna("")
    df.columns = ["c1", "c2", "c3", "business_type"]

    def make_fio(r):
        # Порядок как ты делал: c2 c1 c3
        fio = " ".join(
            [clean_spaces(r["c2"]), clean_spaces(r["c1"]), clean_spaces(r["c3"])]
        )
        return clean_spaces(fio)

    df["fio_key"] = df.apply(make_fio, axis=1).map(normalize_name_key)
    df["business_type"] = df["business_type"].astype(str).map(clean_spaces)

    df = df[df["fio_key"] != ""].copy()
    last = df.drop_duplicates("fio_key", keep="last")
    return dict(zip(last["fio_key"], last["business_type"]))


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


def aggregate_builds_by_project(
    data, sd_people_map, bk_type_map, exclude_without_team=True
):
    acc = defaultdict(int)

    for j in data.get("jobs", []):
        if j.get("isFolder"):
            continue

        full_name = (j.get("name") or "").strip()
        if not full_name:
            continue

        root = full_name.split("/", 1)[0].strip()
        if not root:
            continue

        project_name, team_number = split_project_and_team(root)
        if exclude_without_team and not team_number:
            continue

        acc[(project_name, team_number)] += int(j.get("buildCount") or 0)

    total_builds = sum(acc.values())

    rows = []
    for (project_name, team_number), builds in sorted(
        acc.items(), key=lambda kv: kv[1], reverse=True
    ):
        people = sd_people_map.get(team_number, {"owner": "", "manager": ""})
        owner = people.get("owner", "")
        manager = people.get("manager", "")

        owner_for_report = owner or manager
        business_type = pick_business_type(bk_type_map, owner=owner, manager=manager)

        pct = (builds / total_builds * 100.0) if total_builds > 0 else 0.0

        rows.append(
            [
                business_type,  # Тип бизнеса (по owner/manager)
                project_name,  # Наименование сервиса (как в Jenkins)
                team_number,  # КОД
                owner_for_report,  # Владелец сервиса (или менеджер)
                builds,  # Кол-во билдов
                round(pct, 2),  # %
            ]
        )

    return rows


def export_excel(rows, filename):
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

    wb.save(filename)
    logger.info(f"Excel сохранён: {filename}")


def main():
    try:
        validate_env_and_files()
        sd_people_map = load_sd_people_map(SD_FILE)
        bk_type_map = load_bk_business_type_map(BK_FILE)

        data = get_builds_inventory()

        rows = aggregate_builds_by_project(
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
