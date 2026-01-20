import os
import sys
import logging
import urllib3
from dotenv import load_dotenv
from jenkins_client import JenkinsGroovyClient
from jenkins_scripts import SCRIPT_JOBS
from collections import defaultdict
from jenkins_node import collect_node

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

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
        project_name = j.get("name", "").split("/", 1)[0]
        if not project_name:
            continue
        acc[project_name]["jobs_sum"] += 1
        last_build = j.get("lastBuild")
        if last_build is not None:
            acc[project_name]["build_sum"] += last_build
    return dict(acc)


def autosize_columns(ws, min_w=10, max_w=60):
    for col in range(1, ws.max_column + 1):
        letter = get_column_letter(col)
        max_len = 0
        for cell in ws[letter]:
            if cell.value is None:
                continue
            max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[letter].width = max(min_w, min(max_w, max_len + 2))


def export_excel(rows, filename="inventory.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.title = "inventory"

    headers = ["team_name", "team_number", "project", "jobs_sum", "build_sum", "labels"]
    ws.append(headers)

    for r in rows:
        ws.append([r.get(h, "") for h in headers])

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(ws.max_column)}{ws.max_row}"
    autosize_columns(ws)
    wb.save(filename)
    logger.info(f"Excel сохранён: {filename}")


def build_rows(jobs_n_builds, collected_node):
    rows = []

    for project, sums in jobs_n_builds.items():
        labels = collected_node.get(project) or []
        team_number = labels[0] if labels else project

        row = {
            "team_name": "",
            "team_number": team_number,
            "project": project,
            "jobs_sum": sums.get("jobs_sum", 0),
            "build_sum": sums.get("build_sum", 0),
            "labels": ", ".join(labels) if labels else "",
        }
        rows.append(row)

        logger.info(
            f"project={project} team_number={team_number} "
            f"jobs_sum={row['jobs_sum']} build_sum={row['build_sum']} labels={labels}"
        )

    return rows


def main():
    try:
        jobs = get_jobs()
        collected_node = collect_node()

        jobs_n_builds = get_sum_build_and_jobs(jobs)

        rows = build_rows(jobs_n_builds, collected_node)
        export_excel(rows, filename="inventory.xlsx")

        logger.info("Инвентаризация завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
