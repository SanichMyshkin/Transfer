import os
import time
import logging
from pathlib import Path

import urllib3
import gitlab
from dotenv import load_dotenv

import humanize
from openpyxl import Workbook

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")

OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "gitlab_projects.xlsx")
PROGRESS_EVERY = int(os.getenv("PROGRESS_EVERY", "100"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.02"))
SSL_VERIFY = os.getenv("SSL_VERIFY", "false").lower() in ("1", "true", "yes")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("gitlab_projects")


def die(msg: str, code: int = 2):
    log.error(msg)
    raise SystemExit(code)


def human_size(n: int) -> str:
    try:
        return humanize.naturalsize(int(n or 0), binary=True)
    except Exception:
        return "0 B"


def connect() -> gitlab.Gitlab:
    if not GITLAB_URL:
        die("GITLAB_URL не задан")
    if not GITLAB_TOKEN:
        die("GITLAB_TOKEN не задан")

    log.info(f"Connect GitLab url={GITLAB_URL} ssl_verify={SSL_VERIFY}")
    gl = gitlab.Gitlab(
        GITLAB_URL,
        private_token=GITLAB_TOKEN,
        ssl_verify=SSL_VERIFY,
        timeout=60,
    )
    gl.auth()
    log.info("Auth OK")
    return gl


def fmt_eta(seconds: float) -> str:
    if seconds is None or seconds < 0:
        return "-"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def main():
    gl = connect()
    log.info("Listing projects...")
    projects = list(gl.projects.list(all=True, iterator=True))
    total = len(projects)
    log.info(f"Total projects: {total}")

    out_path = str(Path(OUTPUT_XLSX).resolve())
    wb = Workbook()
    ws = wb.active
    ws.title = "Projects"
    ws.append(["project", "owner", "size"])

    start = time.time()
    errors = 0

    for i, p in enumerate(projects, start=1):
        t0 = time.time()
        try:
            full = gl.projects.get(p.id, statistics=True)

            project_name = full.path_with_namespace or full.name or str(full.id)

            ns = getattr(full, "namespace", {}) or {}
            owner = (ns.get("full_path") or ns.get("name") or "").strip()

            stats = getattr(full, "statistics", {}) or {}
            size_bytes = stats.get("repository_size", 0)

            ws.append([project_name, owner, human_size(size_bytes)])

        except Exception as e:
            errors += 1
            ws.append([getattr(p, "path_with_namespace", "") or str(getattr(p, "id", "")), "", ""])
            log.warning(f"FAIL project_id={getattr(p, 'id', '?')} err={e}")

        if SLEEP_SEC > 0:
            time.sleep(SLEEP_SEC)

        if i == 1 or i % PROGRESS_EVERY == 0 or i == total:
            elapsed = time.time() - start
            rate = i / elapsed if elapsed > 0 else 0.0
            remaining = (total - i) / rate if rate > 0 else None
            last_ms = (time.time() - t0) * 1000.0

            log.info(
                "Progress %d/%d (%.1f%%) | ok=%d err=%d | %.2f proj/s | eta=%s | last=%.0fms",
                i, total, (i / total * 100.0) if total else 0.0,
                i - errors, errors,
                rate, fmt_eta(remaining),
                last_ms,
            )

    wb.save(out_path)
    log.info(f"Saved: {out_path} | rows={total} errors={errors}")


if __name__ == "__main__":
    main()
