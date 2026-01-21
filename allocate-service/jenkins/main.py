import os
import sys
import logging
import urllib3
from dotenv import load_dotenv
from collections import defaultdict
from openpyxl import Workbook
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

SCRIPT_BUILDS = r"""
import jenkins.model.Jenkins
import groovy.json.JsonOutput

def jobs = Jenkins.instance.getAllItems()
def jobList = []

jobs.each { j ->
    def info = [
        name: j.fullName,
        url: (j.metaClass.respondsTo(j, "getAbsoluteUrl") ? j.absoluteUrl : ""),
        type: j.class.simpleName,
        isFolder: j.class.simpleName.contains("Folder")
    ]

    // Считаем только билды
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
        team = parts[-1]
        project = "-".join(parts[:-1])
        return project, team

    return project_raw, ""


def aggregate_builds_by_project(data):
    acc = defaultdict(lambda: {"builds": 0})

    for j in data.get("jobs", []):
        if j.get("isFolder"):
            continue

        full_name = j.get("name", "")
        root = full_name.split("/", 1)[0].strip()
        if not root:
            continue

        acc[root]["builds"] += int(j.get("buildCount") or 0)
    total_builds = sum(v["builds"] for v in acc.values())
    rows = []
    for root, v in sorted(acc.items(), key=lambda kv: kv[1]["builds"], reverse=True):
        project_name, team_number = split_project_and_team(root)
        builds = v["builds"]
        pct = (builds / total_builds * 100.0) if total_builds > 0 else 0.0
        rows.append([project_name, team_number, builds, round(pct, 2)])

    return rows, total_builds


def export_excel(rows, total_builds, filename="jenkins_report.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.title = "Отчет"

    ws.append(
        [
            "Название проекта",
            "Номер команды",
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
        data = get_builds_inventory()
        rows, total_builds = aggregate_builds_by_project(data)
        export_excel(rows, total_builds)
        logger.info("Инвентаризация билдов завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
