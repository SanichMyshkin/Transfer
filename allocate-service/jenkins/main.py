import os
import sys
import logging
import urllib3
from dotenv import load_dotenv
from collections import defaultdict

from openpyxl import Workbook

from jenkins_client import JenkinsGroovyClient
from jenkins_scripts import SCRIPT_JOBS
from jenkins_node import collect_node

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

client = JenkinsGroovyClient(JENKINS_URL, USER, TOKEN, is_https=False)


def get_jobs():
    logger.info("Получаем джобы...")
    data = client.run_script(SCRIPT_JOBS)
    logger.info(f"Джоб: {data['total']}")
    return data


def get_sum_build_and_jobs(data):
    acc = defaultdict(lambda: {"jobs_sum": 0, "build_sum": 0})
    for j in data.get("jobs", []):
        if j.get("isFolder"):
            continue
        project = j.get("name", "").split("/", 1)[0]
        if not project:
            continue
        acc[project]["jobs_sum"] += 1
        if j.get("lastBuild") is not None:
            acc[project]["build_sum"] += j["lastBuild"]
    return dict(acc)


def split_node_name(node_name):
    parts = node_name.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0], parts[1]
    return node_name, ""


def build_rows(jobs_n_builds, collected_node):
    rows = []

    for project, sums in jobs_n_builds.items():
        labels = collected_node.get(project) or []

        if labels:
            node_name = labels[0]
            team_name, team_number = split_node_name(node_name)
        else:
            team_name = project
            team_number = ""

        rows.append([
            team_name,
            team_number,
            project,
            sums.get("jobs_sum", 0),
            sums.get("build_sum", 0),
        ])

        logger.info(
            f"project={project} team_name={team_name} "
            f"team_number={team_number} jobs={sums['jobs_sum']} builds={sums['build_sum']}"
        )

    return rows


def export_excel(rows, filename="inventory.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.title = "inventory"

    ws.append(["team_name", "team_number", "project", "jobs_sum", "build_sum"])

    for row in rows:
        ws.append(row)

    wb.save(filename)
    logger.info(f"Excel сохранён: {filename}")


def main():
    try:
        jobs = get_jobs()
        collected_node = collect_node()
        jobs_n_builds = get_sum_build_and_jobs(jobs)

        rows = build_rows(jobs_n_builds, collected_node)
        export_excel(rows)

        logger.info("Инвентаризация завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
