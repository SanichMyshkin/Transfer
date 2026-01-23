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
SONAR2_URL = os.getenv("SONAR2_URL", "").rstrip("/")
SONAR3_URL = os.getenv("SONAR3_URL", "").rstrip("/")

SONAR_TOKEN = os.getenv("SONAR_TOKEN", "")
SONAR2_TOKEN = os.getenv("SONAR2_TOKEN", "")
SONAR3_TOKEN = os.getenv("SONAR3_TOKEN", "")

OUT_FILE = os.getenv("OUT_FILE", "sonarQube_report.xlsx")
SD_FILE = os.getenv("SD_FILE")
BK_FILE = os.getenv("BK_FILE", "bk_all_users.xlsx")

SKIP_IF_CODE_NOT_IN_SD = True
SKIP_EMPTY_SERVICES = True

BAN_SERVICE_IDS = [15473]


def die(msg: str):
    logger.error(msg)
    raise SystemExit(2)


def build_ban_set(v):
    if not isinstance(v, (list, tuple, set)):
        die("BAN_SERVICE_IDS должен быть list/tuple/set")
    return {str(x).strip() for x in v if str(x).strip()}


ban_set = build_ban_set(BAN_SERVICE_IDS)


def clean(s):
    return " ".join((s or "").replace(",", " ").split())


def norm_key(s):
    return clean(s).lower()


def normalize_code(v):
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return str(int(v))
    s = str(v).strip()
    return s[:-2] if s.endswith(".0") and s[:-2].isdigit() else s


def split_service_name_code(prefix):
    parts = [p for p in prefix.split("-") if p]
    if len(parts) >= 2 and parts[-1].isdigit():
        return "-".join(parts[:-1]), parts[-1]
    return prefix, ""


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
            s, url, "/api/ce/activity",
            {"component": key, "status": "IN_PROGRESS,SUCCESS,FAILED,CANCELED", "p": p, "ps": 100}
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


def load_sd(path):
    if not path or not os.path.exists(path):
        return {}

    wb = load_workbook(path, data_only=True)
    ws = wb.active
    m = {}

    for r in ws.iter_rows(min_row=2, values_only=True):
        code = normalize_code(r[1] if len(r) > 1 else None)
        if not code:
            continue
        m[code] = {
            "name": clean(r[3] if len(r) > 3 else ""),
            "owner": clean(r[7] if len(r) > 7 else ""),
            "manager": clean(r[8] if len(r) > 8 else ""),
        }
    return m


def load_bk(path):
    if not path or not os.path.exists(path):
        return {}

    wb = load_workbook(path, data_only=True)
    ws = wb.active
    m = {}

    for r in ws.iter_rows(min_row=2, values_only=True):
        fio = clean(f"{r[1] or ''} {r[0] or ''} {r[2] or ''}")
        bt = clean(r[44] if len(r) > 44 else "")
        if fio:
            m[norm_key(fio)] = bt
    return m


def pick_bt(bk, owner, manager):
    return bk.get(norm_key(owner)) or bk.get(norm_key(manager)) or ""


def process_sonar(url, token, sd, bk, acc):
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

        svc, code = split_service_name_code(key.split(":", 1)[0])
        if not code or code in ban_set:
            continue
        if SKIP_IF_CODE_NOT_IN_SD and code not in sd:
            continue

        sd_row = sd.get(code, {})
        svc = sd_row.get("name") or svc
        owner = sd_row.get("owner") or sd_row.get("manager")
        bt = pick_bt(bk, sd_row.get("owner"), sd_row.get("manager"))

        tasks = get_tasks(s, url, key)
        tcnt, lines = 0, 0

        for t in tasks:
            tcnt += 1
            pr, br = t.get("pullRequest"), t.get("branch")
            if pr:
                ck = (key, pr)
                new_cache.setdefault(ck, measure(s, url, key, "new_lines", pr=pr))
                lines += new_cache[ck]
            else:
                b = br or "__main__"
                ck = (key, b)
                if ck not in ncloc_cache:
                    ncloc_cache[ck] = measure(s, url, key, "ncloc", branch=None if b == "__main__" else b)
                lines += ncloc_cache[ck]

        if SKIP_EMPTY_SERVICES and not tcnt and not lines:
            continue

        logger.info(
            '[%s] %s (%s) owner="%s" type="%s" tasks=%d lines=%d',
            label, svc, code, owner, bt, tcnt, lines
        )

        acc.setdefault(code, {
            "business_type": bt,
            "service": svc,
            "code": code,
            "owner": owner,
            "tasks_total": 0,
            "total_lines": 0,
        })

        acc[code]["tasks_total"] += tcnt
        acc[code]["total_lines"] += lines


def write_xlsx(rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "Отчет SonarQube"

    headers = [
        "Тип бизнеса",
        "Наименование сервиса",
        "КОД",
        "Владелец сервиса",
        "Кол-во тасок",
        "Обработано кол-во строк",
        "% потребления",
    ]
    ws.append(headers)

    for c in ws[1]:
        c.font = Font(bold=True)

    total = sum(r["total_lines"] for r in rows) or 1
    for r in rows:
        ws.append([
            r["business_type"],
            r["service"],
            r["code"],
            r["owner"],
            r["tasks_total"],
            r["total_lines"],
            round(r["total_lines"] / total * 100, 2),
        ])

    wb.save(OUT_FILE)
    logger.info("Файл сохранён: %s", OUT_FILE)


def main():
    if not SONAR_URL or not SONAR_TOKEN:
        die("SONAR_URL / SONAR_TOKEN не заданы")

    sd = load_sd(SD_FILE)
    bk = load_bk(BK_FILE)

    services = {}

    process_sonar(SONAR_URL, SONAR_TOKEN, sd, bk, services)
    process_sonar(SONAR2_URL, SONAR2_TOKEN, sd, bk, services)
    process_sonar(SONAR3_URL, SONAR3_TOKEN, sd, bk, services)

    rows = sorted(services.values(), key=lambda x: x["total_lines"], reverse=True)
    write_xlsx(rows)


if __name__ == "__main__":
    main()
