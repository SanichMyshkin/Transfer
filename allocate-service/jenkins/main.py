import os
import sys
import logging
import urllib3
from dotenv import load_dotenv
from collections import defaultdict
from openpyxl import Workbook, load_workbook
from jenkins_client import JenkinsGroovyClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

JENKINS_URL = os.getenv("JENKINS_URL")
USER = os.getenv("USER")
TOKEN = os.getenv("TOKEN")

EXCLUDE_PROJECTS_WITHOUT_TEAM_NUMBER = True
BUSINESS_XLSX_PATH = os.getenv("BUSINESS_XLSX_PATH", "buissnes.xlsx")

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


def load_business_mapping(path: str):
    mapping = {}
    if not path or not os.path.exists(path):
        logger.warning(f"Файл buissnes не найден: {path}")
        return mapping

    wb = load_workbook(path, data_only=True)
    ws = wb.active

    for row in ws.iter_rows(min_row=2, values_only=True):
        b = row[1] if len(row) > 1 else None
        c = row[2] if len(row) > 2 else None
        d = row[3] if len(row) > 3 else None

        if b is None:
            continue

        team = str(b).strip()
        if not team:
            continue

        category = str(c).strip() if c is not None else ""
        name = str(d).strip() if d is not None else ""
        mapping[team] = {"category": category, "name": name}

    logger.info(f"Загружено соответствий из buissnes: {len(mapping)}")
    return mapping


def aggregate_builds_by_project(data, business_map, exclude_without_team=True):
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
        bm = business_map.get(team_number or "", {})
        display_name = bm.get("name") or project_name
        category = bm.get("category") or ""
        pct = (builds / total_builds * 100.0) if total_builds > 0 else 0.0
        rows.append([display_name, team_number, category, builds, round(pct, 2)])

    return rows


def export_excel(rows, filename="jenkins_report.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.title = "inventory"
    ws.append(
        [
            "Название команды",
            "Номер команды",
            "Категория",
            "Кол-во билдов",
            "% от общего кол-ва билдов",
        ]
    )
    for r in rows:
        ws.append(r)
    wb.save(filename)
    logger.info(f"Excel сохранён: {filename}")


def main():
    try:
        business_map = load_business_mapping(BUSINESS_XLSX_PATH)
        data = get_builds_inventory()
        rows = aggregate_builds_by_project(
            data,
            business_map=business_map,
            exclude_without_team=EXCLUDE_PROJECTS_WITHOUT_TEAM_NUMBER,
        )
        export_excel(rows, filename="inventory.xlsx")
        logger.info("Инвентаризация билдов завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
