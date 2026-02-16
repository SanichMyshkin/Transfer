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
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.02"))
SSL_VERIFY = False
LOG_EVERY = int(os.getenv("LOG_EVERY", "25"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("gitlab_projects")


def die(msg: str, code: int = 2):
    log.error(msg)
    raise SystemExit(code)


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
        per_page=100,
    )
    gl.auth()
    log.info("Auth OK")
    return gl


def main():
    gl = connect()

    out_path = str(Path(OUTPUT_XLSX).resolve())
    wb = Workbook()
    ws = wb.active
    ws.title = "Projects"
    ws.append(["project", "owner", "size"])

    errors = 0
    start_ts = time.time()

    log.info("Listing projects (stream)...")

    for i, p in enumerate(gl.projects.list(all=True, iterator=True), start=1):
        proj_id = getattr(p, "id", None)
        proj_name = getattr(p, "path_with_namespace", None) or getattr(p, "name", None) or str(proj_id)

        log.info(f"PROJECT i={i} id={proj_id} name={proj_name}")

        try:
            stats = getattr(p, "statistics", None)
            if stats:
                ns = getattr(p, "namespace", {}) or {}
            else:
                full = gl.projects.get(proj_id, statistics=True)
                ns = getattr(full, "namespace", {}) or {}
                stats = getattr(full, "statistics", {}) or {}

            owner = (ns.get("full_path") or ns.get("name") or "").strip()

            size_bytes = int((stats or {}).get("repository_size", 0) or 0)
            ws.append([proj_name, owner, humanize.naturalsize(size_bytes, binary=True)])

        except Exception as e:
            errors += 1
            ws.append([proj_name, "", f"ERROR: {e}"])
            log.warning(f"FAIL i={i} project_id={proj_id} err={e}")

        if LOG_EVERY > 0 and i % LOG_EVERY == 0:
            elapsed = time.time() - start_ts
            rate = i / elapsed if elapsed > 0 else 0
            log.info(f"PROGRESS i={i} rate={rate:.2f} proj/s errors={errors}")

        if SLEEP_SEC > 0:
            time.sleep(SLEEP_SEC)

    wb.save(out_path)
    log.info(f"Saved: {out_path} | rows={ws.max_row - 1} errors={errors}")


if __name__ == "__main__":
    main()
