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

if not SONAR_URL or not TOKEN:
    logger.error("Не заданы SONAR_URL/SONAR_TOKEN")
    raise SystemExit(1)

session = requests.Session()
session.auth = (TOKEN, "")
session.headers.update({"Accept": "application/json"})


def sonar_get(path: str, params: dict):
    url = f"{SONAR_URL}{path}"
    logger.info("API GET %s params=%s", path, params)
    r = session.get(url, params=params, verify=False, timeout=60)
    logger.info("API RESP %s -> %s", path, r.status_code)
    if r.status_code >= 400:
        logger.error("API ERROR %s -> %s body=%s", path, r.status_code, r.text)
    r.raise_for_status()
    return r.json()


def get_projects():
    logger.info("Получаем список проектов...")
    projects = []
    page = 1
    size = 500
    while True:
        data = sonar_get("/api/projects/search", {"p": page, "ps": size})
        batch = data.get("components", [])
        projects.extend(batch)
        total = data.get("paging", {}).get("total", 0)
        logger.info("Проекты: page=%d получили=%d всего=%d", page, len(batch), len(projects))
        if page * size >= total:
            break
        page += 1
    logger.info("Итого проектов: %d", len(projects))
    return projects


def get_ce_tasks(project_key: str):
    logger.info("Получаем CE tasks для проекта: %s", project_key)
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
        batch = data.get("tasks", [])
        tasks.extend(batch)
        total = data.get("paging", {}).get("total", 0)
        logger.info("CE tasks %s: page=%d получили=%d всего=%d", project_key, page, len(batch), len(tasks))
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
        logger.info("MEASURE EMPTY project=%s metric=%s branch=%s pr=%s", project_key, metric, branch, pull_request)
        return 0

    m = measures[0]
    if pull_request:
        v = (m.get("period") or {}).get("value")
    else:
        v = m.get("value")

    if v is None:
        logger.info("MEASURE NO_VALUE project=%s metric=%s branch=%s pr=%s", project_key, metric, branch, pull_request)
        return 0

    try:
        val = int(float(v))
        logger.info("MEASURE OK project=%s metric=%s branch=%s pr=%s value=%s", project_key, metric, branch, pull_request, val)
        return val
    except Exception:
        logger.info("MEASURE BAD_VALUE project=%s metric=%s branch=%s pr=%s raw=%s", project_key, metric, branch, pull_request, v)
        return 0


def parse_service_prefix(project_key: str):
    return project_key.split(":", 1)[0]


def split_service_name_code(prefix: str):
    parts = [p for p in prefix.split("-") if p]
    if len(parts) >= 2 and parts[-1].isdigit():
        return "-".join(parts[:-1]), parts[-1]
    return prefix, ""


def normalize_code(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return ""
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        return str(v).strip()
    s = str(v).strip()
    if s.endswith(".0"):
        s2 = s[:-2]
        if s2.isdigit():
            return s2
    return s


def load_sd_map(path: str):
    if not path:
        logger.info("SD_FILE не задан, маппинг сервисов пропущен")
        return {}
    if not os.path.exists(path):
        logger.info("SD_FILE не найден: %s (маппинг сервисов пропущен)", path)
        return {}

    logger.info("Загружаем SD файл: %s", path)
    wb = load_workbook(path, data_only=True)
    ws = wb.active
    m = {}
    loaded = 0
    skipped = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        code = normalize_code(row[1] if len(row) > 1 else None)
        if not code:
            skipped += 1
            continue
        category = (str(row[2]).strip() if len(row) > 2 and row[2] is not None else "")
        name = (str(row[3]).strip() if len(row) > 3 and row[3] is not None else "")
        m[code] = (category, name)
        loaded += 1
    logger.info("SD маппинг загружен: %d записей (пропущено строк без кода: %d)", loaded, skipped)
    return m


def write_xlsx(rows):
    logger.info("Пишем XLSX отчет: %s", OUT_FILE)

    wb = Workbook()
    ws = wb.active
    ws.title = "Отчет SonarQube (fix)"

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

    total_lines = sum(r["total_lines"] for r in rows)
    if total_lines == 0:
        total_lines = 1

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
    logger.info("Готово. Строк в отчете: %d", len(rows))


def main():
    logger.info("==== START ====")

    sd_map = load_sd_map(SD_FILE)
    projects = get_projects()

    ncloc_cache = {}
    newlines_cache = {}
    services = {}

    for i, p in enumerate(projects, start=1):
        project_key = p.get("key")
        if not project_key:
            continue

        logger.info("Обработка проекта %d/%d: %s", i, len(projects), project_key)

        tasks = get_ce_tasks(project_key)
        logger.info("Проект %s: tasks=%d", project_key, len(tasks))

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

        logger.info("Проект %s: tasks_total=%d branch_lines=%d pr_new_lines=%d", project_key, tasks_total, branch_lines, pr_lines)

        prefix = project_key.split(":", 1)[0]
        svc_name, svc_code = split_service_name_code(prefix)

        category = ""
        if svc_code and svc_code in sd_map:
            category, mapped_name = sd_map[svc_code]
            if mapped_name:
                logger.info("SD match code=%s: name '%s' -> '%s', category='%s'", svc_code, svc_name, mapped_name, category)
                svc_name = mapped_name
            else:
                logger.info("SD match code=%s: category='%s', name not provided", svc_code, category)
        else:
            if svc_code:
                logger.info("SD no match for code=%s (оставляем имя='%s', категория пустая)", svc_code, svc_name)
            else:
                logger.info("Код не выделен из префикса '%s' (оставляем имя='%s', категория пустая)", prefix, svc_name)

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
        services[svc_key]["total_lines"] += branch_lines + pr_lines

        logger.info(
            "Агрегация сервис=%s code=%s category=%s -> tasks_total=%d total_lines=%d",
            services[svc_key]["service"],
            services[svc_key]["code"],
            services[svc_key]["category"],
            services[svc_key]["tasks_total"],
            services[svc_key]["total_lines"],
        )

    rows = list(services.values())
    rows.sort(key=lambda x: x["total_lines"], reverse=True)

    logger.info("Сервисов в отчете: %d", len(rows))
    write_xlsx(rows)

    logger.info("==== DONE ====")


if __name__ == "__main__":
    main()
