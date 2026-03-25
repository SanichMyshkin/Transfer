import os
import logging
import requests
import urllib3
import re

from dotenv import load_dotenv
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

SONAR_URL = os.getenv("SONAR_URL", "").rstrip("/")
SONAR2_URL = os.getenv("SONAR2_URL", "").rstrip("/")

SONAR_TOKEN = os.getenv("SONAR_TOKEN", "")
SONAR2_TOKEN = os.getenv("SONAR2_TOKEN", "")

OUT_FILE = os.getenv("OUT_FILE", "sonarQube_report.xlsx")
ACTIVITY_FILE = os.getenv("ACTIVITY_FILE", "activity.xlsx")

BAN_SERVICE_IDS = {15473}


def clean(s):
    return " ".join((s or "").replace(",", " ").split())


def normalize_code(v):
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return str(int(v))
    s = str(v).strip()
    return s[:-2] if s.endswith(".0") and s[:-2].isdigit() else s


CODE_ANYWHERE_RE = re.compile(r"(?:^|[^0-9])(\d{2,})(?:[^0-9]|$)")


def split_service_name_code(prefix: str):
    p = (prefix or "").strip()
    if not p:
        return "", ""

    m = CODE_ANYWHERE_RE.search(p)
    if not m:
        return p, ""

    return p, m.group(1)


def make_session(token):
    s = requests.Session()
    s.auth = (token, "")
    s.headers.update({"Accept": "application/json"})
    return s


def sonar_get(s, url, path, params):
    r = s.get(f"{url}{path}", params=params, verify=False, timeout=60)
    r.raise_for_status()
    return r.json()


def get_projects(s, url):
    res, p = [], 1
    while True:
        d = sonar_get(s, url, "/api/projects/search", {"p": p, "ps": 500})
        res += d.get("components", [])
        if p * 500 >= d["paging"]["total"]:
            break
        p += 1
    return res


def get_tasks(s, url, key):
    res, p = [], 1
    while True:
        d = sonar_get(
            s,
            url,
            "/api/ce/activity",
            {
                "component": key,
                "status": "IN_PROGRESS,SUCCESS,FAILED,CANCELED",
                "p": p,
                "ps": 100,
            },
        )
        res += d.get("tasks", [])
        if p * 100 >= d["paging"]["total"]:
            break
        p += 1
    return res


def measure(s, url, key, metric, branch=None, pr=None):
    params = {"component": key, "metricKeys": metric}
    if branch:
        params["branch"] = branch
    if pr:
        params["pullRequest"] = str(pr)

    d = sonar_get(s, url, "/api/measures/component", params)
    m = (d.get("component", {}).get("measures") or [{}])[0]
    v = (m.get("period") or {}).get("value") if pr else m.get("value")

    try:
        return int(float(v))
    except Exception:
        return 0


def calc_project_tasks_lines(s, url, key, tasks, ncloc_cache, new_cache):
    tcnt, lines = 0, 0

    for t in tasks:
        tcnt += 1
        pr, br = t.get("pullRequest"), t.get("branch")

        if pr:
            ck = (key, pr)
            if ck not in new_cache:
                new_cache[ck] = measure(s, url, key, "new_lines", pr=pr)
            lines += new_cache[ck]
        else:
            b = br or "__main__"
            ck = (key, b)
            if ck not in ncloc_cache:
                ncloc_cache[ck] = measure(
                    s, url, key, "ncloc", branch=None if b == "__main__" else b
                )
            lines += ncloc_cache[ck]

    return tcnt, lines


def load_activity(path):
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.active

    m = {}

    for r in ws.iter_rows(values_only=True):
        code = normalize_code(r[0] if len(r) > 0 else "")
        if not code:
            continue

        m[code] = {
            "service_name": clean(r[1] if len(r) > 1 else ""),
            "activity_code": clean(r[2] if len(r) > 2 else ""),
            "activity_name": clean(r[3] if len(r) > 3 else ""),
        }

    return m


def process_sonar(url, token, activity, acc, unacc):
    if not url or not token:
        return

    label = url.replace("https://", "").replace("http://", "")
    s = make_session(token)

    projects = get_projects(s, url)
    logger.info("[%s] проектов: %d", label, len(projects))

    ncloc_cache, new_cache = {}, {}

    for p in projects:
        key = p.get("key")
        if not key:
            continue

        prefix = key.split(":", 1)[0]
        svc_guess, code = split_service_name_code(prefix)

        if not code:
            unacc.append(
                dict(
                    instance=label,
                    project_key=key,
                    code="",
                    reason="no_code",
                    detail="cannot parse service_id",
                )
            )
            continue

        if code in BAN_SERVICE_IDS:
            unacc.append(
                dict(
                    instance=label,
                    project_key=key,
                    code=code,
                    reason="banned_service_id",
                )
            )
            continue

        meta = activity.get(code)
        if not meta:
            unacc.append(
                dict(
                    instance=label,
                    project_key=key,
                    code=code,
                    reason="activity_mapping_miss",
                )
            )
            continue

        tasks = get_tasks(s, url, key)
        tcnt, lines = calc_project_tasks_lines(
            s, url, key, tasks, ncloc_cache, new_cache
        )

        acc.setdefault(
            code,
            dict(
                service_name=meta["service_name"],
                code=code,
                activity_code=meta["activity_code"],
                activity_name=meta["activity_name"],
                tasks_total=0,
                total_lines=0,
            ),
        )

        acc[code]["tasks_total"] += tcnt
        acc[code]["total_lines"] += lines


def write_xlsx(rows, unacc):
    wb = Workbook()

    ws = wb.active
    ws.title = "Отчет SonarQube"

    headers = [
        "Имя сервиса",
        "Код",
        "Код активности",
        "Наименование активности",
        "Кол-во тасок",
        "Строки",
        "% потребления",
    ]
    ws.append(headers)

    for c in ws[1]:
        c.font = Font(bold=True)

    total = sum(r["total_lines"] for r in rows) or 1

    for r in rows:
        ws.append(
            [
                r["service_name"],
                r["code"],
                r["activity_code"],
                r["activity_name"],
                r["tasks_total"],
                r["total_lines"],
                r["total_lines"] / total,
            ]
        )

    ws2 = wb.create_sheet("Unaccounted")
    ws2.append(["instance", "project_key", "code", "reason"])

    for r in unacc:
        ws2.append(
            [
                r.get("instance"),
                r.get("project_key"),
                r.get("code"),
                r.get("reason"),
            ]
        )

    wb.save(OUT_FILE)


def main():
    activity = load_activity(ACTIVITY_FILE)

    services = {}
    unaccounted = []

    process_sonar(SONAR_URL, SONAR_TOKEN, activity, services, unaccounted)
    process_sonar(SONAR2_URL, SONAR2_TOKEN, activity, services, unaccounted)

    rows = sorted(services.values(), key=lambda x: x["total_lines"], reverse=True)

    write_xlsx(rows, unaccounted)


if __name__ == "__main__":
    main()