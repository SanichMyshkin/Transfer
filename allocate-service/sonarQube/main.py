import os
import logging
import requests
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

SONAR_URL = os.getenv("SONAR_URL", "").rstrip("/")
TOKEN = os.getenv("SONAR_TOKEN", "")

if not SONAR_URL or not TOKEN:
    logger.error("Не заданы переменные окружения SONAR_URL и SONAR_TOKEN")
    raise SystemExit(1)

session = requests.Session()
session.auth = (TOKEN, "")
session.headers.update({"Accept": "application/json"})


def sonar_get(path: str, params: dict):
    r = session.get(f"{SONAR_URL}{path}", params=params, verify=False, timeout=60)
    r.raise_for_status()
    return r.json()


def get_projects():
    logger.info("Получение проектов...")
    projects = []
    page = 1
    size = 500

    while True:
        logger.info(f"[PROJECTS] GET page={page}")
        data = sonar_get("/api/projects/search", {"p": page, "ps": size})
        batch = data.get("components", [])
        projects.extend(batch)
        logger.info(f"[PROJECTS] Получено {len(batch)} (всего: {len(projects)})")

        total = data.get("paging", {}).get("total", 0)
        if page * size >= total:
            break
        page += 1

    logger.info(f"Проекты получены: {len(projects)}")
    return projects


def get_ce_tasks(project_key: str):
    logger.info(f"[CE TASKS] Получение задач CE для {project_key}")
    tasks_all = []
    page = 1
    size = 100

    while True:
        logger.info(f"[CE TASKS] {project_key} GET page={page}")
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
        tasks_all.extend(batch)
        logger.info(
            f"[CE TASKS] {project_key} Получено {len(batch)}, всего {len(tasks_all)}"
        )

        total = data.get("paging", {}).get("total", 0)
        if page * size >= total:
            break
        page += 1

    return tasks_all


def get_measure_value(
    component_key: str,
    metric: str,
    branch: str | None = None,
    pull_request: str | None = None,
) -> int:
    params = {"component": component_key, "metricKeys": metric}
    if branch:
        params["branch"] = branch
    if pull_request:
        params["pullRequest"] = pull_request

    data = sonar_get("/api/measures/component", params)
    measures = data.get("component", {}).get("measures", [])
    if not measures:
        return 0
    v = measures[0].get("value", "0")
    try:
        return int(float(v))
    except Exception:
        return 0


def main():
    logger.info("==== START ====")

    projects = get_projects()

    ncloc_cache: dict[
        tuple[str, str], int
    ] = {}  # (project_key, branch|__main__) -> ncloc
    newlines_cache: dict[tuple[str, str], int] = {}  # (project_key, pr) -> new_lines

    report = []

    for p in projects:
        project_key = p.get("key")
        project_name = p.get("name") or project_key
        if not project_key:
            continue

        tasks = get_ce_tasks(project_key)

        total_tasks = 0
        pr_tasks = 0
        branch_tasks = 0

        total_lines_checked = 0
        total_pr_new_lines = 0
        total_branch_ncloc = 0

        for t in tasks:
            total_tasks += 1

            pr = t.get("pullRequest")
            branch = t.get("branch")

            if pr:
                pr_tasks += 1
                cache_key = (project_key, str(pr))
                if cache_key not in newlines_cache:
                    newlines_cache[cache_key] = get_measure_value(
                        project_key, "new_lines", pull_request=str(pr)
                    )
                nl = newlines_cache[cache_key]
                total_lines_checked += nl
                total_pr_new_lines += nl
                continue

            branch_tasks += 1
            branch_name = branch if branch else "__main__"
            cache_key = (project_key, branch_name)
            if cache_key not in ncloc_cache:
                if branch:
                    ncloc_cache[cache_key] = get_measure_value(
                        project_key, "ncloc", branch=branch
                    )
                else:
                    ncloc_cache[cache_key] = get_measure_value(project_key, "ncloc")
            ncloc = ncloc_cache[cache_key]
            total_lines_checked += ncloc
            total_branch_ncloc += ncloc

        report.append(
            {
                "project": project_name,
                "key": project_key,
                "tasks_total": total_tasks,
                "tasks_branch": branch_tasks,
                "tasks_pr": pr_tasks,
                "lines_checked_total": total_lines_checked,
                "lines_checked_branch_ncloc_sum": total_branch_ncloc,
                "lines_checked_pr_new_lines_sum": total_pr_new_lines,
            }
        )

        logger.info(
            f"[REPORT] {project_key}: tasks={total_tasks} (branch={branch_tasks}, pr={pr_tasks}), "
            f"lines={total_lines_checked} (branch_sum={total_branch_ncloc}, pr_sum={total_pr_new_lines})"
        )

    report.sort(key=lambda x: x["lines_checked_total"], reverse=True)

    print("\n==== RESULT ====")
    for row in report:
        print(
            f"{row['project']} | tasks={row['tasks_total']} (branch={row['tasks_branch']}, pr={row['tasks_pr']}) | "
            f"lines_total={row['lines_checked_total']} | branch_sum={row['lines_checked_branch_ncloc_sum']} | "
            f"pr_sum={row['lines_checked_pr_new_lines_sum']} | key={row['key']}"
        )

    logger.info("==== DONE ====")


if __name__ == "__main__":
    main()
