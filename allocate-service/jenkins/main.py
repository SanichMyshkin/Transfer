import os
import sys
import logging
import urllib3
from dotenv import load_dotenv
from collections import defaultdict
from difflib import SequenceMatcher

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from jenkins_client import JenkinsGroovyClient
from jenkins_scripts import SCRIPT_JOBS, SCRIPT_NODES


logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
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
    logger.info(f"Джоб: {data.get('total')}")
    return data


def get_nodes():
    logger.info("Получаем ноды...")
    data = client.run_script(SCRIPT_NODES)
    logger.info(f"Нод: {data.get('total')}")
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


def extract_node_names(nodes_data):
    names = []
    for n in nodes_data.get("nodes", []):
        name = n.get("name")
        if isinstance(name, str) and name.strip():
            names.append(name.strip())
    return names


def norm(s):
    return "".join(ch.lower() for ch in s.strip() if ch.isalnum() or ch in "-_.")


def best_match(project_name, node_names, min_ratio=0.72):
    pn = norm(project_name)
    best = (None, 0.0)
    for nn in node_names:
        r = SequenceMatcher(None, pn, norm(nn)).ratio()
        if r > best[1]:
            best = (nn, r)
    if best[0] is None or best[1] < min_ratio:
        return None, 0.0
    return best


def build_rows(jobs_summary, node_names):
    rows = []

    used_nodes = set()

    for project, sums in jobs_summary.items():
        node, ratio = best_match(project, node_names)
        if node:
            used_nodes.add(node)

        rows.append({
            "team_name": "",
            "team_number": node or "",
            "project": project,
            "match_ratio": round(ratio, 3) if node else "",
            "jobs_sum": sums.get("jobs_sum", 0),
            "build_sum": sums.get("build_sum", 0),
        })

        logger.info(
            f"Project={project} node={'-' if not node else node} "
            f"ratio={'-' if not node else round(ratio, 3)} "
            f"jobs={sums.get('jobs_sum', 0)} builds={sums.get('build_sum', 0)}"
        )

    # если хочешь добавлять ноды без джоб — раскомментируй блок ниже
    # for node in node_names:
    #     if node in used_nodes:
    #         continue
    #     rows.append({
    #         "team_name": "",
    #         "team_number": node,
    #         "project": "",
    #         "match_ratio": "",
    #         "jobs_sum": 0,
    #         "build_sum": 0,
    #     })

    return rows


def autosize_columns(ws, min_w=10, max_w=60):
    for col in range(1, ws.max_column + 1):
        letter = get_column_letter(col)
        max_len = 0
        for cell in ws[letter]:
            if cell.value is None:
                continue
            max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[letter].width = max(min_w, min(max_w, max_len + 2))


def export_to_excel(rows, filename="inventory.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.title = "inventory"

    headers = ["team_name", "team_number", "project", "match_ratio", "jobs_sum", "build_sum"]
    ws.append(headers)

    for r in rows:
        ws.append([r.get(h, "") for h in headers])

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(ws.max_column)}{ws.max_row}"
    autosize_columns(ws)

    wb.save(filename)
    logger.info(f"Excel сохранён: {filename}")


def main():
    try:
        jobs = get_jobs()
        nodes = get_nodes()

        jobs_summary = get_sum_build_and_jobs(jobs)
        node_names = extract_node_names(nodes)

        logger.info(f"Уникальных проектов (jobs): {len(jobs_summary)}")
        logger.info(f"Нод с именами: {len(node_names)}")

        rows = build_rows(jobs_summary, node_names)
        export_to_excel(rows, filename="inventory.xlsx")

        logger.info("Инвентаризация завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
