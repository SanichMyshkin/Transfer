import os
import re
import time
import logging
from pathlib import Path

import urllib3
import gitlab
import humanize
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

GITLAB_URL = (os.getenv("GITLAB_URL") or "").rstrip("/")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN") or ""

OUT_XLSX = os.getenv("OUT_XLSX", "gitlab_pr_with_service_id.xlsx")

SSL_VERIFY = False
SLEEP_SEC = 0.02
LOG_EVERY = 100

SERVICE_ID_RE = re.compile(r"^service_id:\s*(\d+)\s*$", re.IGNORECASE)

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


def extract_service_id(topics) -> str:
    for t in (topics or []):
        s = (t or "").strip()
        m = SERVICE_ID_RE.match(s)
        if m:
            return m.group(1)
    return ""


def autosize_columns(ws, max_width=80):
    for col_idx in range(1, ws.max_column + 1):
        letter = get_column_letter(col_idx)
        best = 0
        for row_idx in range(1, ws.max_row + 1):
            v = ws.cell(row=row_idx, column=col_idx).value
            if v is None:
                continue
            best = max(best, len(str(v)))
        ws.column_dimensions[letter].width = min(max(10, best + 2), max_width)


def main():
    gl = connect()

    out_path = str(Path(OUT_XLSX).resolve())
    wb = Workbook()
    ws = wb.active
    ws.title = "Projects"

    headers = [
        "project_id",
        "project",
        "web_url",
        "service_id",
        "repo_size_h",
        "job_artifacts_h",
        "total_h",
    ]
    ws.append(headers)
    for c in ws[1]:
        c.font = Font(bold=True)

    errors = 0
    start_ts = time.time()

    log.info("Начинаем обход всех проектов...")

    for i, p in enumerate(gl.projects.list(all=True, iterator=True), start=1):
        proj_id = getattr(p, "id", None)

        try:
            full = gl.projects.get(proj_id, statistics=True)
            name = getattr(full, "path_with_namespace", "") or getattr(full, "name", "") or str(proj_id)
            web_url = getattr(full, "web_url", "") or ""

            topics = list(getattr(full, "topics", []) or [])
            service_id = extract_service_id(topics)

            stats = getattr(full, "statistics", {}) or {}
            repo_bytes = int(stats.get("repository_size", 0) or 0)
            job_bytes = int(stats.get("job_artifacts_size", 0) or 0)
            total_bytes = repo_bytes + job_bytes

            ws.append(
                [
                    proj_id,
                    name,
                    web_url,
                    service_id,
                    humanize.naturalsize(repo_bytes, binary=True),
                    humanize.naturalsize(job_bytes, binary=True) if job_bytes else "",
                    humanize.naturalsize(total_bytes, binary=True),
                ]
            )

        except Exception as e:
            errors += 1
            proj_name = getattr(p, "path_with_namespace", "") or getattr(p, "name", "") or str(proj_id)
            log.warning(f'FAIL project_id={proj_id} project="{proj_name}" err={e}')

        if LOG_EVERY and i % LOG_EVERY == 0:
            elapsed = time.time() - start_ts
            rate = i / elapsed if elapsed > 0 else 0.0
            log.info(f"PROGRESS i={i} rate={rate:.2f}/s errors={errors}")

        if SLEEP_SEC:
            time.sleep(SLEEP_SEC)

    autosize_columns(ws)
    wb.save(out_path)
    log.info(f"Saved: {out_path} | rows={ws.max_row - 1} errors={errors}")
    log.info("Готово")


if __name__ == "__main__":
    main()
