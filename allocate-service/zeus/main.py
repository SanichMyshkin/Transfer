# main.py
import os
import json
import logging
import urllib3

import gitlab
import yaml
from dotenv import load_dotenv

from openpyxl import Workbook
from openpyxl.styles import Font

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("zeus_deploy_resources")

load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL", "").rstrip("/")
TOKEN = os.getenv("TOKEN", "")
GROUP_ID = os.getenv("GROUP_ID", "").strip()

OUTPUT_XLSX = "zeus_deploy_resources.xlsx"
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


def get_team_name_from_gitlab_json(proj, ref: str) -> str | None:
    try:
        root = repo_tree(proj)
        gitlab_dir = next(
            (i for i in root if i["type"] == "tree" and i["name"] == "gitlab"), None
        )
        if not gitlab_dir:
            return None

        lvl2 = repo_tree(proj, gitlab_dir["path"])
        mapping = next(
            (i for i in lvl2 if i["type"] == "tree" and i["name"] == "mapping"), None
        )
        if not mapping:
            return None

        items = repo_tree(proj, mapping["path"])
        jf = next(
            (i for i in items if i["type"] == "blob" and i["name"] == "gitlab.json"),
            None,
        )
        if not jf:
            return None

        txt = get_file_text(proj, jf["path"], ref)
        data = json.loads(txt)
        name = (data.get("group", {}) or {}).get("name")
        name = (name or "").strip()
        return name or None
    except Exception as e:
        log.warning(f"[{proj.path_with_namespace}] Не смог прочитать gitlab.json: {e}")
        return None


def find_deployment_files(proj):
    """
    Аналогично monitors:
    - ищем zeus-* папку
    - внутри подпапок ищем файлы *-deployment.yml / *-deployment.yaml
    """
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

    files = []
    try:
        zeus_items = repo_tree(proj, zeus_dir["path"])
        subfolders = [i for i in zeus_items if i["type"] == "tree"]
    except Exception as e:
        log.warning(f"[{proj.path_with_namespace}] Не смог прочитать zeus-* дерево: {e}")
        return []

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


def parse_deployment_yaml(text: str, project_name: str, file_path: str):
    """
    Эталонные YAML, без табов/мусора.
    Поддерживаем multi-doc (---) и выбираем kind: Deployment.
    Возвращаем список контейнеров с resources.
    """
    try:
        docs = list(yaml.safe_load_all(text))
    except Exception as e:
        log.warning(f"[{project_name}] YAML не распарсился: {file_path} ({e})")
        return []

    out = []
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        if (doc.get("kind") or "").strip() != "Deployment":
            continue

        meta = doc.get("metadata") or {}
        dep_name = (meta.get("name") or "").strip()
        namespace = (meta.get("namespace") or "").strip()

        spec = doc.get("spec") or {}
        tmpl = (spec.get("template") or {}).get("spec") or {}
        containers = tmpl.get("containers") or []
        if not isinstance(containers, list):
            continue

        for c in containers:
            if not isinstance(c, dict):
                continue

            cname = (c.get("name") or "").strip()
            res = c.get("resources") or {}
            req = res.get("requests") or {}
            lim = res.get("limits") or {}

            out.append(
                {
                    "deployment": dep_name,
                    "namespace": namespace,
                    "container": cname,
                    "cpu_request": req.get("cpu"),
                    "memory_request": req.get("memory"),
                    "cpu_limit": lim.get("cpu"),
                    "memory_limit": lim.get("memory"),
                }
            )

    return out


def collect_rows(gl, projects, ref: str):
    rows = []
    processed = 0

    for p in projects:
        processed += 1
        try:
            proj = gl.projects.get(p.id)
        except Exception as e:
            log.warning(f"[{p.name}] Не смог получить project объект: {e}")
            continue

        service = get_team_name_from_gitlab_json(proj, ref) or p.name

        files = find_deployment_files(proj)
        log.info(f"[{p.name}] service='{service}' deployment_files={len(files)}")

        for path in files:
            try:
                raw = get_file_text(proj, path, ref)
            except Exception as e:
                log.warning(f"[{p.name}] Не смог прочитать {path} ({ref}): {e}")
                continue

            entries = parse_deployment_yaml(raw, p.name, path)
            if not entries:
                continue

            for e in entries:
                rows.append(
                    {
                        "service": service,
                        "project": p.name,
                        "file_path": path,
                        "deployment": e["deployment"],
                        "namespace": e["namespace"],
                        "container": e["container"],
                        "cpu_request": e["cpu_request"],
                        "memory_request": e["memory_request"],
                        "cpu_limit": e["cpu_limit"],
                        "memory_limit": e["memory_limit"],
                    }
                )

    log.info(f"Проектов обработано: {processed}, строк в отчете: {len(rows)}")
    return rows


def write_excel(rows, out_file: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "DeployResources"

    headers = [
        "Наименование сервиса",
        "project",
        "file_path",
        "deployment",
        "namespace",
        "container",
        "cpu_request",
        "memory_request",
        "cpu_limit",
        "memory_limit",
    ]

    bold = Font(bold=True)
    for col, h in enumerate(headers, start=1):
        c = ws.cell(row=1, column=col, value=h)
        c.font = bold

    for i, r in enumerate(rows, start=2):
        ws.cell(i, 1, r["service"])
        ws.cell(i, 2, r["project"])
        ws.cell(i, 3, r["file_path"])
        ws.cell(i, 4, r["deployment"])
        ws.cell(i, 5, r["namespace"])
        ws.cell(i, 6, r["container"])
        ws.cell(i, 7, r["cpu_request"])
        ws.cell(i, 8, r["memory_request"])
        ws.cell(i, 9, r["cpu_limit"])
        ws.cell(i, 10, r["memory_limit"])

    wb.save(out_file)
    log.info(f"Excel отчет создан: {out_file}")


def main():
    log.info("=== START: GitLab deployment resources report ===")
    gl = gl_connect()
    projects = get_group_projects(gl)
    rows = collect_rows(gl, projects, ref=GIT_REF)
    write_excel(rows, OUTPUT_XLSX)
    log.info("=== DONE ===")


if __name__ == "__main__":
    main()
