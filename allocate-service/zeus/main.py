# main.py
import os
import logging
import urllib3

import gitlab
import yaml
from dotenv import load_dotenv

from openpyxl import Workbook
from openpyxl.styles import Font

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("deploy_limits_report")

load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL", "").rstrip("/")
TOKEN = os.getenv("TOKEN", "")
GROUP_ID = os.getenv("GROUP_ID", "").strip()

OUTPUT_XLSX = "deploy_limits_report.xlsx"
GIT_REF = "main"


def gl_connect():
    log.info("Подключаемся к GitLab...")
    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN, ssl_verify=False, timeout=60)
    gl.auth()
    log.info("GitLab: ok")
    return gl


def get_group_projects(gl):
    log.info(f"Получаем проекты группы GROUP_ID={GROUP_ID} (включая сабгруппы)...")
    group = gl.groups.get(GROUP_ID)
    projs = group.projects.list(all=True, include_subgroups=True)
    log.info(f"Проектов найдено: {len(projs)}")
    return projs


def repo_tree(proj, path=None):
    if path:
        return proj.repository_tree(path=path, all=True)
    return proj.repository_tree(all=True)


def get_file_text(proj, file_path: str, ref: str) -> str:
    f = proj.files.get(file_path=file_path, ref=ref)
    return f.decode().decode("utf-8")


def find_deployment_files(proj):
    # как было с monitors, только -deployment.yml/.yaml
    try:
        root = repo_tree(proj)
    except Exception as e:
        log.warning(f"[{proj.path_with_namespace}] repository_tree error: {e}")
        return []

    zeus_dir = next(
        (i for i in root if i["type"] == "tree" and i["name"].startswith("zeus-")),
        None,
    )
    if not zeus_dir:
        return []

    try:
        zeus_items = repo_tree(proj, zeus_dir["path"])
        subfolders = [i for i in zeus_items if i["type"] == "tree"]
    except Exception as e:
        log.warning(f"[{proj.path_with_namespace}] Не смог прочитать zeus-* дерево: {e}")
        return []

    files = []
    for sub in subfolders:
        try:
            sub_items = repo_tree(proj, sub["path"])
        except Exception as e:
            log.warning(f"[{proj.path_with_namespace}] tree({sub['path']}) error: {e}")
            continue

        for f in sub_items:
            if f["type"] != "blob":
                continue
            n = (f.get("name") or "").lower()
            if n.endswith("-deployment.yaml") or n.endswith("-deployment.yml"):
                files.append(f["path"])

    return files


def parse_cpu_to_cores(v):
    # "1300m" -> 1.3 ; "1" -> 1.0
    if v is None:
        return 0.0
    s = str(v).strip()
    if not s:
        return 0.0
    if s.endswith("m"):
        num = s[:-1].strip()
        try:
            return float(num) / 1000.0
        except Exception:
            return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def parse_mem_to_bytes(v):
    # "600Mi" / "1Gi" / "500M" etc.
    if v is None:
        return 0
    s = str(v).strip()
    if not s:
        return 0

    units = {
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "Pi": 1024**5,
        "Ei": 1024**6,
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
        "P": 1000**5,
        "E": 1000**6,
    }

    for u, mul in units.items():
        if s.endswith(u):
            num = s[: -len(u)].strip()
            try:
                return int(float(num) * mul)
            except Exception:
                return 0

    try:
        return int(float(s))
    except Exception:
        return 0


def bytes_to_mib(b: int) -> float:
    return float(b) / (1024.0**2)


def parse_deployment_limits(text: str, project_name: str, file_path: str):
    # возвращаем сумму лимитов по файлу: (cpu_cores, mem_bytes)
    try:
        docs = list(yaml.safe_load_all(text))
    except Exception as e:
        log.warning(f"[{project_name}] YAML не распарсился: {file_path} ({e})")
        return 0.0, 0

    cpu_sum = 0.0
    mem_sum = 0

    for doc in docs:
        if not isinstance(doc, dict):
            continue
        if (doc.get("kind") or "").strip() != "Deployment":
            continue

        spec = doc.get("spec") or {}
        tmpl = (spec.get("template") or {}).get("spec") or {}
        containers = tmpl.get("containers") or []
        if not isinstance(containers, list):
            continue

        for c in containers:
            if not isinstance(c, dict):
                continue
            res = c.get("resources") or {}
            lim = res.get("limits") or {}

            cpu_sum += parse_cpu_to_cores(lim.get("cpu"))
            mem_sum += parse_mem_to_bytes(lim.get("memory"))

    return cpu_sum, mem_sum


def collect_project_totals(gl, projects):
    totals = {}  # project_name -> {"cpu": float cores, "mem": int bytes}

    for p in projects:
        try:
            proj = gl.projects.get(p.id)
        except Exception as e:
            log.warning(f"[{p.name}] Не смог получить project объект: {e}")
            continue

        files = find_deployment_files(proj)
        log.info(f"[{p.name}] deployment_files={len(files)}")

        cpu_total = 0.0
        mem_total = 0

        for path in files:
            try:
                raw = get_file_text(proj, path, GIT_REF)
            except Exception as e:
                log.warning(f"[{p.name}] Не смог прочитать {path} ({GIT_REF}): {e}")
                continue

            cpu, mem = parse_deployment_limits(raw, p.name, path)
            cpu_total += cpu
            mem_total += mem

        if cpu_total > 0 or mem_total > 0:
            totals[p.name] = {"cpu": cpu_total, "mem": mem_total}

    return totals


def write_excel(project_totals, out_file: str):
    # общий total для процентов
    total_cpu = sum(v["cpu"] for v in project_totals.values())
    total_mem = sum(v["mem"] for v in project_totals.values())

    rows = []
    for project, v in project_totals.items():
        cpu = v["cpu"]
        mem = v["mem"]

        cpu_pct = (cpu / total_cpu * 100.0) if total_cpu > 0 else 0.0
        mem_pct = (mem / total_mem * 100.0) if total_mem > 0 else 0.0

        # как ты описал: (cpu% + mem%) / 2
        pct = (cpu_pct + mem_pct) / 2.0

        rows.append(
            {
                "project": project,
                "cpu_cores": cpu,
                "mem_mib": bytes_to_mib(mem),
                "pct": pct,
            }
        )

    rows.sort(key=lambda r: r["pct"], reverse=True)

    wb = Workbook()
    ws = wb.active
    ws.title = "Report"

    headers = ["ПРОЕКТ", "CPU (cores)", "MEM (MiB)", "% потребления"]
    bold = Font(bold=True)
    for col, h in enumerate(headers, start=1):
        c = ws.cell(row=1, column=col, value=h)
        c.font = bold

    for i, r in enumerate(rows, start=2):
        ws.cell(i, 1, r["project"])
        ws.cell(i, 2, round(r["cpu_cores"], 6))
        ws.cell(i, 3, round(r["mem_mib"], 2))
        ws.cell(i, 4, round(r["pct"], 2))

    wb.save(out_file)
    log.info(f"Excel отчет создан: {out_file}")


def main():
    log.info("=== START: Deployment limits report ===")
    gl = gl_connect()
    projects = get_group_projects(gl)
    totals = collect_project_totals(gl, projects)
    log.info(f"Проектов с найденными лимитами: {len(totals)}")
    write_excel(totals, OUTPUT_XLSX)
    log.info("=== DONE ===")


if __name__ == "__main__":
    main()
