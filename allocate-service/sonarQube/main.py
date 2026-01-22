import os
import logging
import requests
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
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
    if r.status_code == 403:
        logger.error("403 %s params=%s body=%s", path, params, r.text)
    r.raise_for_status()
    return r.json()


def get_projects():
    projects = []
    page = 1
    size = 500
    while True:
        data = sonar_get("/api/projects/search", {"p": page, "ps": size})
        batch = data.get("components", [])
        projects.extend(batch)
        total = data.get("paging", {}).get("total", 0)
        logger.info(
            "[PROJECTS] page=%d got=%d total=%d", page, len(batch), len(projects)
        )
        if page * size >= total:
            break
        page += 1
    return projects


def get_ce_tasks(project_key: str):
    tasks_all = []
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
        tasks_all.extend(batch)
        total = data.get("paging", {}).get("total", 0)
        if page * size >= total:
            break
        page += 1
    return tasks_all


def measure_value(
    project_key: str,
    metric: str,
    branch: str | None = None,
    pull_request: str | None = None,
) -> int:
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
    if pull_request:
        v = (m.get("period") or {}).get("value")
    else:
        v = m.get("value")

    if v is None:
        return 0

    try:
        return int(float(v))
    except Exception:
        return 0


def parse_service_prefix(project_key: str) -> str:
    return (project_key.split(":", 1)[0] if project_key else "").strip()


def split_service_name_code(prefix: str):
    parts = [p for p in prefix.split("-") if p != ""]
    if len(parts) >= 2 and parts[-1].isdigit():
        code = parts[-1]
        name = "-".join(parts[:-1])
        return name, code
    return prefix, ""


def main():
    logger.info("==== START ====")
    projects = get_projects()

    ncloc_cache = {}
    newlines_cache = {}

    per_project = []
    per_service = {}

    for p in projects:
        project_key = p.get("key")
        project_name = p.get("name") or project_key
        if not project_key:
            continue

        tasks = get_ce_tasks(project_key)

        tasks_total = 0
        tasks_branch = 0
        tasks_pr = 0

        lines_branch_sum = 0
        lines_pr_sum = 0

        for t in tasks:
            tasks_total += 1
            pr = t.get("pullRequest")
            branch = t.get("branch")

            if pr:
                tasks_pr += 1
                ck = (project_key, str(pr))
                if ck not in newlines_cache:
                    newlines_cache[ck] = measure_value(
                        project_key, "new_lines", pull_request=str(pr)
                    )
                lines_pr_sum += newlines_cache[ck]
            else:
                tasks_branch += 1
                b = branch if branch else "__main__"
                ck = (project_key, b)
                if ck not in ncloc_cache:
                    if b == "__main__":
                        ncloc_cache[ck] = measure_value(project_key, "ncloc")
                    else:
                        ncloc_cache[ck] = measure_value(project_key, "ncloc", branch=b)
                lines_branch_sum += ncloc_cache[ck]

        per_project.append(
            {
                "project": project_name,
                "key": project_key,
                "tasks_total": tasks_total,
                "tasks_branch": tasks_branch,
                "tasks_pr": tasks_pr,
                "branch_lines_sum": lines_branch_sum,
                "pr_new_lines_sum": lines_pr_sum,
            }
        )

        svc_prefix = parse_service_prefix(project_key)
        svc_name, svc_code = split_service_name_code(svc_prefix)
        svc_key = (svc_name.lower(), svc_code)

        if svc_key not in per_service:
            per_service[svc_key] = {
                "service": svc_name,
                "code": svc_code,
                "projects_count": 0,
                "tasks_total": 0,
                "tasks_branch": 0,
                "tasks_pr": 0,
                "branch_lines_sum": 0,
                "pr_new_lines_sum": 0,
            }

        agg = per_service[svc_key]
        agg["projects_count"] += 1
        agg["tasks_total"] += tasks_total
        agg["tasks_branch"] += tasks_branch
        agg["tasks_pr"] += tasks_pr
        agg["branch_lines_sum"] += lines_branch_sum
        agg["pr_new_lines_sum"] += lines_pr_sum

    service_rows = list(per_service.values())
    service_rows.sort(
        key=lambda x: (x["branch_lines_sum"] + x["pr_new_lines_sum"]), reverse=True
    )

    print("\n==== SERVICES RESULT ====")
    for r in service_rows:
        total_lines = r["branch_lines_sum"] + r["pr_new_lines_sum"]
        code_part = f"-{r['code']}" if r["code"] else ""
        print(
            f"{r['service']}{code_part} | projects={r['projects_count']} | "
            f"tasks={r['tasks_total']} (branch={r['tasks_branch']}, pr={r['tasks_pr']}) | "
            f"branch_lines={r['branch_lines_sum']} | pr_new_lines={r['pr_new_lines_sum']} | total_lines={total_lines}"
        )

    logger.info("==== DONE ====")


if __name__ == "__main__":
    main()
