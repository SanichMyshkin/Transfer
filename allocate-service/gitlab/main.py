import os
import time
import logging
from pathlib import Path

import urllib3
import gitlab
from dotenv import load_dotenv

import humanize
from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "gitlab_projects.xlsx")

SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.05"))
SSL_VERIFY = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("gitlab_projects_3cols")


def die(msg: str, code: int = 2):
    log.error(msg)
    raise SystemExit(code)


def human_size(n: int) -> str:
    try:
        n = int(n or 0)
    except Exception:
        n = 0
    return humanize.naturalsize(n, binary=True)


def autosize(ws):
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            v = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, len(v))
        ws.column_dimensions[col_letter].width = min(max_len + 2, 60)


def get_gitlab_connection() -> gitlab.Gitlab:
    if not GITLAB_URL:
        die("Не задан GITLAB_URL")
    if not GITLAB_TOKEN:
        die("Не задан GITLAB_TOKEN")

    gl = gitlab.Gitlab(
        GITLAB_URL,
        private_token=GITLAB_TOKEN,
        ssl_verify=SSL_VERIFY,
        timeout=60,
    )
    gl.auth()
    return gl


def collect_rows(gl: gitlab.Gitlab):
    rows = []
    projects = gl.projects.list(all=True, iterator=True)

    for idx, p in enumerate(projects, start=1):
        try:
            full = gl.projects.get(p.id, statistics=True)

            project_name = full.path_with_namespace or full.name

            ns = getattr(full, "namespace", {}) or {}
            owner = (ns.get("full_path") or ns.get("name") or "").strip()

            stats = getattr(full, "statistics", {}) or {}
            repo_size_bytes = stats.get("repository_size", 0)

            rows.append((project_name, owner, human_size(repo_size_bytes)))

            if idx % 200 == 0:
                log.info(f"Обработано проектов: {idx}")

            time.sleep(SLEEP_SEC)

        except Exception as e:
            log.warning(f"Ошибка проекта {getattr(p, 'id', '?')}: {e}")
            continue

    return rows


def write_excel(rows, filename: str):
    filename = str(Path(filename).resolve())
    wb = Workbook()
    ws = wb.active
    ws.title = "Projects"

    headers = ["project", "owner", "repo_size"]
    ws.append(headers)
    for c in range(1, 4):
        ws.cell(row=1, column=c).font = Font(bold=True)

    for row in rows:
        ws.append(list(row))

    ws.freeze_panes = "A2"
    autosize(ws)
    wb.save(filename)
    log.info(f"Excel сохранён: {filename}")
    return filename


def main():
    gl = get_gitlab_connection()
    rows = collect_rows(gl)
    write_excel(rows, OUTPUT_XLSX)
    log.info("Готово.")


if __name__ == "__main__":
    main()
