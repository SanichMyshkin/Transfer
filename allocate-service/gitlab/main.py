import os
import re
import time
import logging
from pathlib import Path
from collections import defaultdict

import urllib3
import gitlab
import humanize
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

GITLAB_URL = (os.getenv("GITLAB_URL") or "").rstrip("/")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN") or ""

OUT_XLSX = os.getenv("OUT_XLSX", "gitlab_report.xlsx")

SSL_VERIFY = False
SLEEP_SEC = 0.02
LOG_EVERY = 100
LIMIT = 0  # 0 = без лимита

BAN_SERVICE_IDS = {
    # "15473",
    # "0",
}

SERVICE_ID_RE = re.compile(r"^service[_-]?id\s*:\s*(\d+)\s*$", re.IGNORECASE)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("gitlab_sizes")


def die(msg: str, code: int = 2):
    log.error(msg)
    raise SystemExit(code)


def connect():
    if not GITLAB_URL:
        die("GITLAB_URL не задан")
    if not GITLAB_TOKEN:
        die("GITLAB_TOKEN не задан")

    gl = gitlab.Gitlab(
        GITLAB_URL,
        private_token=GITLAB_TOKEN,
        ssl_verify=SSL_VERIFY,
        timeout=60,
        per_page=100,
    )
    gl.auth()
    log.info("Подключение к GitLab успешно")
    return gl


def extract_service_id_info(topics):
    ids = []

    for t in (topics or []):
        s = (t or "").strip()
        m = SERVICE_ID_RE.match(s)
        if m:
            ids.append(m.group(1))

    if not ids:
        return "", "MISSING"

    uniq = sorted(set(ids))
    if len(uniq) == 1:
        return uniq[0], "OK"

    return "", "CONFLICT"


def pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return (part / total) * 100.0


def main():
    gl = connect()

    out_path = str(Path(OUT_XLSX).resolve())
    wb = Workbook()

    # Лист 1 — агрегация
    ws_sum = wb.active
    ws_sum.title = "ByServiceId"

    ws_sum.append(
        [
            "service_id",
            "projects_count",
            "repo_size_h_sum",
            "job_artifacts_h_sum",
            "total_h_sum",
            "percent_of_all_total",
        ]
    )
    for c in ws_sum[1]:
        c.font = Font(bold=True)

    # Лист 2 — не сматчилось
    ws_unmapped = wb.create_sheet("Unmapped")
    ws_unmapped.append(
        [
            "project_id",
            "project",
            "web_url",
            "repo_size_h",
            "job_artifacts_h",
            "total_h",
        ]
    )
    for c in ws_unmapped[1]:
        c.font = Font(bold=True)

    agg = defaultdict(lambda: {"projects": 0, "repo": 0, "job": 0})

    total_repo_all = 0
    total_job_all = 0

    errors = 0
    processed = 0

    log.info("Начинаем обход проектов...")

    for i, p in enumerate(gl.projects.list(all=True, iterator=True), start=1):

        if LIMIT and processed >= LIMIT:
            log.info(f"LIMIT={LIMIT} достигнут, останавливаемся")
            break

        proj_id = getattr(p, "id", None)

        try:
            full = gl.projects.get(proj_id, statistics=True)
            name = getattr(full, "path_with_namespace", "") or getattr(full, "name", "") or str(proj_id)
            web_url = getattr(full, "web_url", "") or ""

            topics = list(getattr(full, "topics", []) or [])
            service_id, sid_status = extract_service_id_info(topics)

            if sid_status == "OK" and service_id in BAN_SERVICE_IDS:
                processed += 1
                continue

            stats = getattr(full, "statistics", {}) or {}
            repo_bytes = int(stats.get("repository_size", 0) or 0)
            job_bytes = int(stats.get("job_artifacts_size", 0) or 0)
            total_bytes = repo_bytes + job_bytes

            total_repo_all += repo_bytes
            total_job_all += job_bytes

            if sid_status == "OK":
                a = agg[service_id]
                a["projects"] += 1
                a["repo"] += repo_bytes
                a["job"] += job_bytes
            else:
                ws_unmapped.append(
                    [
                        proj_id,
                        name,
                        web_url,
                        humanize.naturalsize(repo_bytes, binary=True),
                        humanize.naturalsize(job_bytes, binary=True) if job_bytes else "",
                        humanize.naturalsize(total_bytes, binary=True),
                    ]
                )

            processed += 1

        except Exception as e:
            errors += 1
            log.warning(f'FAIL project_id={proj_id} err={e}')

        if LOG_EVERY and i % LOG_EVERY == 0:
            log.info(f"PROGRESS i={i} errors={errors}")

        if SLEEP_SEC:
            time.sleep(SLEEP_SEC)

    total_all = total_repo_all + total_job_all

    for service_id in sorted(agg.keys(), key=lambda x: int(x) if x.isdigit() else x):
        repo_b = agg[service_id]["repo"]
        job_b = agg[service_id]["job"]
        total_b = repo_b + job_b

        ws_sum.append(
            [
                service_id,
                agg[service_id]["projects"],
                humanize.naturalsize(repo_b, binary=True),
                humanize.naturalsize(job_b, binary=True) if job_b else "",
                humanize.naturalsize(total_b, binary=True),
                round(pct(total_b, total_all), 4),
            ]
        )

    wb.save(out_path)

    log.info(
        "Saved: %s | projects=%d | services=%d | unmapped=%d | errors=%d | total=%s",
        out_path,
        processed,
        len(agg),
        ws_unmapped.max_row - 1,
        errors,
        humanize.naturalsize(total_all, binary=True),
    )
    log.info("Готово")


if __name__ == "__main__":
    main()