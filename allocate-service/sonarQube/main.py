import os
import logging
import requests
import urllib3
from dotenv import load_dotenv
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
SONAR_URL = os.getenv("SONAR_URL", "").rstrip("/")
TOKEN = os.getenv("SONAR_TOKEN", "")
OUT_FILE = os.getenv("OUT_FILE", "sonarQube_report.xlsx")
SD_FILE = os.getenv("SD_FILE")

SKIP_IF_CODE_NOT_IN_SD = True
SKIP_EMPTY_SERVICES = True

if not SONAR_URL or not TOKEN:
    logger.error("Не заданы SONAR_URL/SONAR_TOKEN")
    raise SystemExit(1)

session = requests.Session()
session.auth = (TOKEN, "")
session.headers.update({"Accept": "application/json"})


def sonar_get(path: str, params: dict):
    r = session.get(f"{SONAR_URL}{path}", params=params, verify=False, timeout=60)
    r.raise_for_status()
    return r.json()


def get_projects():
    projects = []
    page = 1
    size = 500
    while True:
        data = sonar_get("/api/projects/search", {"p": page, "ps": size})
        projects.extend(data.get("components", []))
        total = data.get("paging", {}).get("total", 0)
        if page * size >= total:
            break
        page += 1
    logger.info("Проектов получено: %d", len(projects))
    return projects


def get_ce_tasks(project_key: str):
    tasks = []
    page = 1
    size = 100
    while True:
        data = sonar_get(
            "/api/ce/activity",
            {
                "component": project_key,
                "status": "IN_PROGRESS,SUCCESS,FAILED,CANCELED",
                "p": page,
                "ps": size,
            },
        )
        tasks.extend(data.get("tasks", []))
        total = data.get("paging", {}).get("total", 0)
        if page * size >= total:
            break
        page += 1
    return tasks


def measure_value(project_key: str, metric: str, branch=None, pull_request=None):
    params = {"component": project_key, "metricKeys": metric}
    if branch:
        params["branch"] = branch
    if pull_request:
        params["pullRequest"] = str(pull_request)

    data = sonar_get("/api/measures/component", params)
    measures = data.get("component", {}).get("measures", [])
    if not measures:
        return 0

    m = measures[0]
    v = (m.get("period") or {}).get("value") if pull_request else m.get("value")
    if v is None:
        return 0

    try:
        return int(float(v))
    except Exception:
        return 0


def split_service_name_code(prefix: str):
    parts = [p for p in prefix.split("-") if p]
    if len(parts) >= 2 and parts[-1].isdigit():
        return "-".join(parts[:-1]), parts[-1]
    return prefix, ""


def normalize_code(v):
    if v is None:
        return ""
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    s = str(v).strip()
    if s.endswith(".0"):
        s2 = s[:-2]
        if s2.isdigit():
            return s2
    return s


def load_sd_map(path: str):
    if not path:
        logger.info("SD_FILE не задан, маппинг пустой")
        return {}
    if not os.path.exists(path):
        logger.info("SD_FILE не найден: %s, маппинг пустой", path)
        return {}

    wb = load_workbook(path, data_only=True)
    ws = wb.active
    m = {}
    for row in ws.iter_rows(min_row=2, values_only=True):
        code = normalize_code(row[1] if len(row) > 1 else None)
        if not code:
            continue
        category = str(row[2]).strip() if len(row) > 2 and row[2] is not None else ""
        name = str(row[3]).strip() if len(row) > 3 and row[3] is not None else ""
        m[code] = (category, name)
    logger.info("SD маппинг: %d кодов", len(m))
    return m


def write_xlsx(rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "Отчет SonarQube"

    headers = [
        "Наименование сервиса",
        "КОД",
        "Категория",
        "Кол-во тасок",
        "Обработано кол-во строк",
        "% потребления",
    ]

    ws.append(headers)
    bold = Font(bold=True)
    for c in range(1, len(headers) + 1):
        ws.cell(row=1, column=c).font = bold

    total_lines = sum(r["total_lines"] for r in rows) or 1

    for r in rows:
        pct = round((r["total_lines"] / total_lines) * 100, 2)
        ws.append([
            r["service"],
            r["code"],
            r["category"],
            r["tasks_total"],
            r["total_lines"],
            f"{pct}%",
        ])

    wb.save(OUT_FILE)
    logger.info("Отчет сохранен: %s (строк: %d)", OUT_FILE, len(rows))


def main():
    logger.info("==== START ====")
    logger.info("SKIP_IF_CODE_NOT_IN_SD=%s", SKIP_IF_CODE_NOT_IN_SD)
    logger.info("SKIP_EMPTY_SERVICES=%s", SKIP_EMPTY_SERVICES)

    sd_map = load_sd_map(SD_FILE)
    projects = get_projects()

    ncloc_cache = {}
    newlines_cache = {}
    services = {}

    skipped_no_code = 0
    skipped_no_sd_match = 0
    skipped_empty_project = 0

    for idx, p in enumerate(projects, start=1):
        project_key = p.get("key")
        if not project_key:
            continue

        prefix = project_key.split(":", 1)[0]
        svc_name, svc_code = split_service_name_code(prefix)

        if not svc_code:
            skipped_no_code += 1
            logger.info("SKIP project=%s (не выделен код)", project_key)
            continue

        if SKIP_IF_CODE_NOT_IN_SD and svc_code not in sd_map:
            skipped_no_sd_match += 1
            logger.info("SKIP project=%s (код %s не найден в SD)", project_key, svc_code)
            continue

        category = ""
        if svc_code in sd_map:
            category, mapped_name = sd_map[svc_code]
            if mapped_name:
                svc_name = mapped_name

        tasks = get_ce_tasks(project_key)

        tasks_total = 0
        branch_lines = 0
        pr_lines = 0

        for t in tasks:
            tasks_total += 1
            pr = t.get("pullRequest")
            branch = t.get("branch")

            if pr:
                ck = (project_key, str(pr))
                if ck not in newlines_cache:
                    newlines_cache[ck] = measure_value(project_key, "new_lines", pull_request=str(pr))
                pr_lines += newlines_cache[ck]
            else:
                b = branch if branch else "__main__"
                ck = (project_key, b)
                if ck not in ncloc_cache:
                    if b == "__main__":
                        ncloc_cache[ck] = measure_value(project_key, "ncloc")
                    else:
                        ncloc_cache[ck] = measure_value(project_key, "ncloc", branch=b)
                branch_lines += ncloc_cache[ck]

        total_lines = branch_lines + pr_lines

        if SKIP_EMPTY_SERVICES and tasks_total == 0 and total_lines == 0:
            skipped_empty_project += 1
            logger.info("SKIP project=%s (пусто: tasks=0 lines=0)", project_key)
            continue

        logger.info(
            "AGG project=%s -> service=%s code=%s cat=%s tasks=%d lines=%d",
            project_key, svc_name, svc_code, category, tasks_total, total_lines
        )

        svc_key = (svc_name.lower(), svc_code, category)

        if svc_key not in services:
            services[svc_key] = {
                "service": svc_name,
                "code": svc_code,
                "category": category,
                "tasks_total": 0,
                "total_lines": 0,
            }

        services[svc_key]["tasks_total"] += tasks_total
        services[svc_key]["total_lines"] += total_lines

    rows = list(services.values())

    if SKIP_EMPTY_SERVICES:
        rows = [r for r in rows if not (r["tasks_total"] == 0 and r["total_lines"] == 0)]

    rows.sort(key=lambda x: x["total_lines"], reverse=True)

    logger.info(
        "Итог: сервисов=%d | skipped(no_code)=%d | skipped(no_sd_match)=%d | skipped(empty_project)=%d",
        len(rows), skipped_no_code, skipped_no_sd_match, skipped_empty_project
    )

    write_xlsx(rows)
    logger.info("==== DONE ====")


if __name__ == "__main__":
    main()
