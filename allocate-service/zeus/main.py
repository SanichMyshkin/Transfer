# main.py
import os
import logging
import urllib3
import re

import gitlab
import yaml
import pandas as pd
from dotenv import load_dotenv

from openpyxl import Workbook
from openpyxl.styles import Font

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("zeus_report")

load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL", "").rstrip("/")
TOKEN = os.getenv("TOKEN", "")
GROUP_ID = os.getenv("GROUP_ID", "").strip()

SD_FILE = os.getenv("SD_FILE")
BK_FILE = os.getenv("BK_FILE")

OUTPUT_XLSX = "zeus_report.xlsx"
GIT_REF = "main"


BAN_SERVICES = [
    "UNAITP",
    "TEST",
]


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


def load_sd_people_map(path: str):
    log.info("Читаем SD (B=КОД, D=Наименование, H=Владелец, I=Менеджер)...")

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

    log.info(f"SD: загружено сервисов по КОД: {len(mp)}")
    return mp


def load_bk_business_type_map(path: str):
    log.info("Читаем BK (A:B:C=ФИО, AS=Тип бизнеса)...")

    df = pd.read_excel(path, usecols="A:C,AS", dtype=str, engine="openpyxl")
    df = df.fillna("")
    df.columns = ["c1", "c2", "c3", "business_type"]

    def make_fio(r):
        fio = " ".join(
            [clean_spaces(r["c2"]), clean_spaces(r["c1"]), clean_spaces(r["c3"])]
        )
        return clean_spaces(fio)

    df["fio_key"] = df.apply(make_fio, axis=1).map(normalize_name_key)
    df["business_type"] = df["business_type"].astype(str).map(clean_spaces)

    df = df[df["fio_key"] != ""].copy()
    last = df.drop_duplicates("fio_key", keep="last")

    mp = dict(zip(last["fio_key"], last["business_type"]))
    log.info(f"BK: загружено ФИО->Тип бизнеса: {len(mp)}")
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
        log.warning(
            f"[{proj.path_with_namespace}] Не смог прочитать zeus-* дерево: {e}"
        )
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
    if v is None:
        return 0.0
    s = str(v).strip()
    if not s:
        return 0.0
    if s.endswith("m"):
        try:
            return float(s[:-1].strip()) / 1000.0
        except Exception:
            return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def parse_mem_to_bytes(v):
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
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
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


def split_service_and_code(project_name: str):
    name = (project_name or "").strip()
    m = re.match(r"^(.*?)-(\d+)$", name)
    if not m:
        return name, ""
    return m.group(1), m.group(2)


def is_zero_code(code: str) -> bool:
    if not code:
        return False
    return set(code) == {"0"}


def collect_service_totals(gl, projects, sd_people_map, bk_type_map):
    totals = {}  # (service, code) -> {"cpu": float, "mem": int}

    for p in projects:
        try:
            proj = gl.projects.get(p.id)
        except Exception as e:
            log.warning(f"[{p.name}] Не смог получить project объект: {e}")
            continue

        service, code = split_service_and_code(p.name)

        if is_zero_code(code):
            log.info(f"[{p.name}] SKIP (code is all zeros)")
            continue

        if service in BAN_SERVICES:
            log.info(f"[{p.name}] SKIP (ban service): service='{service}'")
            continue

        files = find_deployment_files(proj)
        log.info(
            f"[p={p.name}] service='{service}' code='{code}' deployment_files={len(files)}"
        )

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

        if cpu_total <= 0 and mem_total <= 0:
            continue

        key = (service, code)
        if key not in totals:
            totals[key] = {"cpu": 0.0, "mem": 0}
        totals[key]["cpu"] += cpu_total
        totals[key]["mem"] += mem_total

    rows = []
    total_cpu = sum(v["cpu"] for v in totals.values())
    total_mem = sum(v["mem"] for v in totals.values())

    for (service, code), v in totals.items():
        people = sd_people_map.get(
            code, {"service_name": "", "owner": "", "manager": ""}
        )
        owner = people.get("owner", "")
        manager = people.get("manager", "")

        owner_for_report = owner or manager
        business_type = pick_business_type(bk_type_map, owner=owner, manager=manager)

        cpu = v["cpu"]
        mem = v["mem"]

        cpu_pct = (cpu / total_cpu * 100.0) if total_cpu > 0 else 0.0
        mem_pct = (mem / total_mem * 100.0) if total_mem > 0 else 0.0
        pct = (cpu_pct + mem_pct) / 2.0

        rows.append(
            {
                "business_type": business_type,
                "service": service,
                "code": code,
                "owner": owner_for_report,
                "cpu_cores": cpu,
                "mem_mib": bytes_to_mib(mem),
                "pct": pct,
            }
        )

    rows.sort(key=lambda r: r["pct"], reverse=True)
    return rows


def write_excel(rows, out_file: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "Report"

    headers = [
        "Тип бизнеса",
        "Наименование сервиса",
        "код",
        "Владелец сервиса",
        "CPU (cores)",
        "MEM (MiB)",
        "% потребления",
    ]

    bold = Font(bold=True)
    for col, h in enumerate(headers, start=1):
        c = ws.cell(row=1, column=col, value=h)
        c.font = bold

    for i, r in enumerate(rows, start=2):
        ws.cell(i, 1, r["service"])
        ws.cell(i, 2, r["code"])
        ws.cell(i, 3, r["owner"])
        ws.cell(i, 4, r["business_type"])
        ws.cell(i, 5, round(r["cpu_cores"], 6))
        ws.cell(i, 6, round(r["mem_mib"], 2))
        ws.cell(i, 7, round(r["pct"], 2))

    wb.save(out_file)
    log.info(f"Excel отчет создан: {out_file}")


def main():
    if not GITLAB_URL or not TOKEN or not GROUP_ID:
        raise SystemExit("Нужны ENV: GITLAB_URL, TOKEN, GROUP_ID")

    if not SD_FILE or not os.path.isfile(SD_FILE):
        raise SystemExit(f"SD_FILE не найден: {SD_FILE}")
    if not BK_FILE or not os.path.isfile(BK_FILE):
        raise SystemExit(f"BK_FILE не найден: {BK_FILE}")

    sd_people_map = load_sd_people_map(SD_FILE)
    bk_type_map = load_bk_business_type_map(BK_FILE)

    gl = gl_connect()
    projects = get_group_projects(gl)

    rows = collect_service_totals(
        gl, projects, sd_people_map=sd_people_map, bk_type_map=bk_type_map
    )

    log.info(f"Строк в отчете: {len(rows)}")
    write_excel(rows, OUTPUT_XLSX)


if __name__ == "__main__":
    main()
