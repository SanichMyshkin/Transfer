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

SONAR_TOKEN = os.getenv("SONAR_TOKEN", "")
SONAR2_TOKEN = os.getenv("SONAR2_TOKEN", "")

OUT_FILE = os.getenv("OUT_FILE", "sonarQube_report.xlsx")
SD_FILE = os.getenv("SD_FILE")
BK_FILE = os.getenv("BK_FILE", "bk_all_users.xlsx")

SKIP_IF_CODE_NOT_IN_SD = True
SKIP_EMPTY_SERVICES = True

BAN_SERVICE_IDS = [15473]
BAN_BUSINESS_TYPES = []
SKIP_EMPTY_BUSINESS_TYPE = True


def die(msg: str):
    logger.error(msg)
    raise SystemExit(2)


def build_ban_set(v):
    if not isinstance(v, (list, tuple, set)):
        die("BAN_SERVICE_IDS должен быть list/tuple/set")
    return {str(x).strip() for x in v if str(x).strip()}


ban_set = build_ban_set(BAN_SERVICE_IDS)
ban_business_set = {
    " ".join(str(x).replace(",", " ").split())
    for x in BAN_BUSINESS_TYPES
    if " ".join(str(x).replace(",", " ").split())
}


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


def pick_bt(bk, owner):
    return clean(bk.get(norm_key(owner)) if owner else "")


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
                ncloc_cache[ck] = measure(s, url, key, "ncloc", branch=None if b == "__main__" else b)
            lines += ncloc_cache[ck]
    return tcnt, lines


def process_sonar(url, token, sd, bk, acc, unaccounted):
    if not url or not token:
        return

    label = url.replace("https://", "").replace("http://", "")
    s = make_session(token)

    projects = get_projects(s, url)
    logger.info("[%s] проектов: %d", label, len(projects))

    ncloc_cache, new_cache = {}, {}

    def add_unacc(reason, detail, project_key, prefix, svc_guess, code, owner, bt, tasks_total, total_lines):
        unaccounted.append({
            "instance": label,
            "project_key": project_key or "",
            "prefix": prefix or "",
            "svc_guess": svc_guess or "",
            "code": code or "",
            "owner": owner or "",
            "business_type": bt or "",
            "tasks_total": int(tasks_total or 0),
            "total_lines": int(total_lines or 0),
            "reason": reason,
            "detail": detail,
        })

    for p in projects:
        key = p.get("key")
        if not key:
            continue

        prefix = key.split(":", 1)[0]
        svc_guess, code = split_service_name_code(prefix)

        # По дефолту tasks/lines неизвестны (не тратим API), но для некоторых причин считаем (чтобы “неучтенные тоже посчитать”)
        tasks_total = 0
        total_lines = 0

        if not code:
            add_unacc(
                reason="no_code_in_key",
                detail=f"cannot parse code from prefix={prefix!r} (expected ...-<digits>)",
                project_key=key,
                prefix=prefix,
                svc_guess=svc_guess,
                code="",
                owner="",
                bt="",
                tasks_total=0,
                total_lines=0,
            )
            continue

        if code in ban_set:
            add_unacc(
                reason="banned_service_id",
                detail=f"code={code} in BAN_SERVICE_IDS",
                project_key=key,
                prefix=prefix,
                svc_guess=svc_guess,
                code=code,
                owner="",
                bt="",
                tasks_total=0,
                total_lines=0,
            )
            continue

        if SKIP_IF_CODE_NOT_IN_SD and code not in sd:
            add_unacc(
                reason="code_not_in_sd",
                detail=f"SKIP_IF_CODE_NOT_IN_SD=True and code={code} not found in SD",
                project_key=key,
                prefix=prefix,
                svc_guess=svc_guess,
                code=code,
                owner="",
                bt="",
                tasks_total=0,
                total_lines=0,
            )
            continue

        sd_row = sd.get(code, {})
        svc = sd_row.get("name") or svc_guess
        owner = sd_row.get("owner") or ""
        bt = pick_bt(bk, owner)

        # Здесь уже считаем usage (tasks/lines) один раз — и для accounted, и для unaccounted
        tasks = get_tasks(s, url, key)
        tasks_total, total_lines = calc_project_tasks_lines(s, url, key, tasks, ncloc_cache, new_cache)

        if SKIP_EMPTY_SERVICES and not tasks_total and not total_lines:
            add_unacc(
                reason="empty_service_usage",
                detail="SKIP_EMPTY_SERVICES=True and tasks_total=0 and total_lines=0",
                project_key=key,
                prefix=prefix,
                svc_guess=svc,
                code=code,
                owner=owner,
                bt=bt,
                tasks_total=tasks_total,
                total_lines=total_lines,
            )
            continue

        if SKIP_EMPTY_BUSINESS_TYPE and not bt:
            det = "owner empty in SD" if not owner else "owner not found in BK or BK business_type empty"
            add_unacc(
                reason="empty_business_type",
                detail=f"SKIP_EMPTY_BUSINESS_TYPE=True and business_type empty ({det})",
                project_key=key,
                prefix=prefix,
                svc_guess=svc,
                code=code,
                owner=owner,
                bt=bt,
                tasks_total=tasks_total,
                total_lines=total_lines,
            )
            continue

        if ban_business_set and bt in ban_business_set:
            add_unacc(
                reason="banned_business_type",
                detail=f"business_type={bt!r} in BAN_BUSINESS_TYPES",
                project_key=key,
                prefix=prefix,
                svc_guess=svc,
                code=code,
                owner=owner,
                bt=bt,
                tasks_total=tasks_total,
                total_lines=total_lines,
            )
            continue

        logger.info(
            '[%s] %s (%s) owner="%s" type="%s" tasks=%d lines=%d',
            label, svc, code, owner, bt, tasks_total, total_lines
        )

        acc.setdefault(code, {
            "business_type": bt,
            "service": svc,
            "code": code,
            "owner": owner,
            "tasks_total": 0,
            "total_lines": 0,
        })

        acc[code]["tasks_total"] += tasks_total
        acc[code]["total_lines"] += total_lines


def write_xlsx(rows, unaccounted_rows):
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

    total_accounted_lines = sum(r["total_lines"] for r in rows) or 0
    total_unaccounted_lines = sum(r.get("total_lines", 0) for r in unaccounted_rows) or 0
    total_all_lines = total_accounted_lines + total_unaccounted_lines
    if total_all_lines <= 0:
        total_all_lines = 1

    for r in rows:
        pct = (r["total_lines"] / total_all_lines) if total_all_lines else 0.0
        ws.append([
            r["business_type"],
            r["service"],
            r["code"],
            r["owner"],
            int(r["tasks_total"]),
            int(r["total_lines"]),
            pct,  # доля, форматируем как %
        ])

    # percent format (0.3% etc)
    pct_col = headers.index("% потребления") + 1
    for rr in range(2, ws.max_row + 1):
        ws.cell(row=rr, column=pct_col).number_format = "0.0%"

    ws2 = wb.create_sheet("Unaccounted")
    headers2 = [
        "instance",
        "project_key",
        "prefix",
        "svc_guess",
        "code",
        "owner",
        "business_type",
        "tasks_total",
        "total_lines",
        "% от total_lines_all",
        "reason",
        "detail",
    ]
    ws2.append(headers2)
    for c in ws2[1]:
        c.font = Font(bold=True)

    pct2_col = headers2.index("% от total_lines_all") + 1

    for r in unaccounted_rows:
        lines = int(r.get("total_lines", 0) or 0)
        pct_all = (lines / total_all_lines) if total_all_lines else 0.0
        ws2.append([
            r.get("instance", ""),
            r.get("project_key", ""),
            r.get("prefix", ""),
            r.get("svc_guess", ""),
            r.get("code", ""),
            r.get("owner", ""),
            r.get("business_type", ""),
            int(r.get("tasks_total", 0) or 0),
            lines,
            pct_all,  # доля
            r.get("reason", ""),
            r.get("detail", ""),
        ])

    for rr in range(2, ws2.max_row + 1):
        ws2.cell(row=rr, column=pct2_col).number_format = "0.0%"

    wb.save(OUT_FILE)
    logger.info("Файл сохранён: %s", OUT_FILE)
    logger.info(
        "Totals: accounted_lines=%d unaccounted_lines=%d total_lines_all=%d",
        total_accounted_lines, total_unaccounted_lines, total_all_lines
    )


def main():
    if not SONAR_URL or not SONAR_TOKEN:
        die("SONAR_URL / SONAR_TOKEN не заданы")

    sd = load_sd(SD_FILE)
    bk = load_bk(BK_FILE)

    services = {}
    unaccounted = []

    process_sonar(SONAR_URL, SONAR_TOKEN, sd, bk, services, unaccounted)
    process_sonar(SONAR2_URL, SONAR2_TOKEN, sd, bk, services, unaccounted)

    rows = sorted(services.values(), key=lambda x: x["total_lines"], reverse=True)
    unaccounted_sorted = sorted(unaccounted, key=lambda x: int(x.get("total_lines", 0) or 0), reverse=True)

    write_xlsx(rows, unaccounted_sorted)


if __name__ == "__main__":
    main()