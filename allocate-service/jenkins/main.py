import os
import sys
import logging
import urllib3
from dotenv import load_dotenv
from collections import defaultdict
from openpyxl import Workbook, load_workbook

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
BUSINESS_XLSX = os.getenv("BUSINESS_XLSX", "business.xlsx")

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


def norm(s):
    s = (s or "").lower()
    return "".join(ch for ch in s if ch.isalnum())


def find_node_key(project, collected_node):
    p = norm(project)
    for k in collected_node.keys():
        nk = norm(k)
        if p and nk and (p in nk or nk in p):
            return k
    return None


def split_name_number(s):
    parts = (s or "").rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0], parts[1]
    return s or "", ""


def hnorm(s):
    return norm(str(s or ""))


def load_business_index(xlsx_path):
    if not xlsx_path or not os.path.exists(xlsx_path):
        logger.info(f"business xlsx not found: {xlsx_path}")
        return {}

    wb = load_workbook(xlsx_path, data_only=True)
    ws = wb.active

    header_row = None
    for r in range(1, min(ws.max_row, 30) + 1):
        vals = [ws.cell(row=r, column=c).value for c in range(1, ws.max_column + 1)]
        if any(v is not None for v in vals):
            header_row = r
            break

    if header_row is None:
        return {}

    headers = {}
    for c in range(1, ws.max_column + 1):
        v = ws.cell(row=header_row, column=c).value
        key = hnorm(v)
        if key:
            headers[key] = c

    def pick_col(*names):
        for n in names:
            k = hnorm(n)
            for hk, col in headers.items():
                if hk == k or k in hk or hk in k:
                    return col
        return None

    col_num = pick_col("team_number", "номер", "номеркоманды", "командаid", "id")
    col_team = pick_col("team_name", "команда", "названиекоманды", "team")
    col_bus = pick_col("business_name", "business", "бизнес", "названиебизнеса", "бизнеснаправление")

    if not col_num:
        logger.info("business xlsx: не нашёл колонку номера команды (team_number/номер/...)")
        return {}

    idx = {}
    for r in range(header_row + 1, ws.max_row + 1):
        num = ws.cell(row=r, column=col_num).value
        if num is None:
            continue
        num_s = str(num).strip()
        if not num_s.isdigit():
            num_s = "".join(ch for ch in num_s if ch.isdigit())
        if not num_s:
            continue

        team_name = ""
        business_name = ""

        if col_team:
            v = ws.cell(row=r, column=col_team).value
            if v is not None:
                team_name = str(v).strip()

        if col_bus:
            v = ws.cell(row=r, column=col_bus).value
            if v is not None:
                business_name = str(v).strip()

        idx[num_s] = {"team_name": team_name, "business_name": business_name}

    logger.info(f"business xlsx loaded: {len(idx)} rows")
    return idx


def business_lookup(biz_idx, team_number):
    if not team_number:
        return "", ""
    rec = biz_idx.get(str(team_number))
    if not rec:
        return "", ""
    return rec.get("business_name", "") or "", rec.get("team_name", "") or ""


def build_rows(jobs_n_builds, collected_node, biz_idx):
    rows = []
    logger.info(f"Всего нод (ключей): {len(collected_node)}")

    for project, sums in jobs_n_builds.items():
        node_key = find_node_key(project, collected_node)
        labels = collected_node.get(node_key) if node_key else []
        labels = labels or []
        nodes_count = len(labels)

        if node_key:
            node_team_name, team_number = split_name_number(node_key)
            fallback_team_name = node_team_name
        else:
            fallback_team_name = project
            _, team_number = split_name_number(project)

        business_name = ""
        team_name = ""

        if team_number:
            business_name, team_name = business_lookup(biz_idx, team_number)
            if not team_name:
                team_name = fallback_team_name
        else:
            team_name = fallback_team_name

        rows.append([
            business_name,
            team_name,
            team_number,
            project,
            sums.get("jobs_sum", 0),
            sums.get("build_sum", 0),
            nodes_count,
        ])

        logger.info(
            f"project={project} node={node_key or '-'} team_number={team_number or '-'} "
            f"nodes_count={nodes_count} business={'+' if business_name else '-'} team={'+' if team_name else '-'}"
        )

    return rows


def export_excel(rows, filename="inventory.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.title = "inventory"

    ws.append([
        "business_name",
        "team_name",
        "team_number",
        "project",
        "jobs_sum",
        "build_sum",
        "nodes_count",
    ])

    for r in rows:
        ws.append(r)

    wb.save(filename)
    logger.info(f"Excel сохранён: {filename}")


def main():
    try:
        jobs = get_jobs()
        collected_node = collect_node()
        biz_idx = load_business_index(BUSINESS_XLSX)

        jobs_n_builds = get_sum_build_and_jobs(jobs)
        rows = build_rows(jobs_n_builds, collected_node, biz_idx)
        export_excel(rows)

        logger.info("Инвентаризация завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
