# tag_projects_simple.py

import os
import time
import logging
import urllib3
import pandas as pd
import gitlab
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

INPUT_XLSX = "gitlab_projects_detailed.xlsx"

PROJECT_ID_COL_IDX = 0
TAG_COL_IDX = 1

DRY_RUN = True
SSL_VERIFY = False
LIMIT = 0  # 0 = без лимита
SLEEP_SEC = 0.1

GITLAB_URL = (os.getenv("GITLAB_URL") or "").rstrip("/")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN") or ""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("gitlab_tag_marker")


def die(msg: str):
    log.error(msg)
    raise SystemExit(2)


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
    )
    gl.auth()
    log.info("Подключение к GitLab успешно")
    return gl


def parse_tags(raw: str):
    s = (raw or "").strip()
    if not s:
        return []

    parts = []
    for chunk in s.replace(";", ",").split(","):
        t = chunk.strip()
        if t:
            parts.append(t)

    return list(dict.fromkeys(parts))


def main():
    gl = connect()

    if not os.path.isfile(INPUT_XLSX):
        die(f"Файл не найден: {INPUT_XLSX}")

    df = pd.read_excel(INPUT_XLSX, dtype=str, engine="openpyxl").fillna("")
    log.info(f"Строк в файле: {len(df)}")

    tasks = []

    for i in range(len(df)):
        raw_id = str(df.iat[i, PROJECT_ID_COL_IDX]).strip()
        raw_tag = str(df.iat[i, TAG_COL_IDX]).strip()

        if not raw_id:
            log.warning(f"SKIP_ROW idx={i + 2} reason=empty_project_id")
            continue

        try:
            proj_id = int(float(raw_id))
        except Exception:
            log.warning(f'SKIP_ROW idx={i + 2} reason=bad_project_id value="{raw_id}"')
            continue

        tags = parse_tags(raw_tag)
        if not tags:
            log.warning(f"SKIP_ROW idx={i + 2} project_id={proj_id} reason=empty_tag")
            continue

        tasks.append((proj_id, tags))

    if LIMIT > 0:
        tasks = tasks[:LIMIT]
        log.info(f"LIMIT включён: {len(tasks)} строк")

    log.info(f"DRY_RUN={DRY_RUN}")

    for proj_id, new_tags in tasks:
        try:
            p = gl.projects.get(proj_id)
            name = getattr(p, "path_with_namespace", str(proj_id))

            current = list(getattr(p, "tag_list", []) or [])
            current_set = set(current)

            to_add = [t for t in new_tags if t not in current_set]

            if not to_add:
                log.info(f'SKIP_ALREADY_HAS project_id={proj_id} project="{name}"')
                continue

            final_tags = current + to_add

            if DRY_RUN:
                log.info(
                    f'DRY_RUN_ADD project_id={proj_id} project="{name}" '
                    f"add={to_add} final={final_tags}"
                )
            else:
                p.tag_list = final_tags
                p.save()
                log.info(f'APPLIED project_id={proj_id} project="{name}" add={to_add}')

        except gitlab.exceptions.GitlabGetError as e:
            log.warning(f"SKIP_MISSING project_id={proj_id} err={e}")
        except Exception as e:
            log.error(f"FAIL project_id={proj_id} err={e}")

        if SLEEP_SEC:
            time.sleep(SLEEP_SEC)

    log.info("Готово")


if __name__ == "__main__":
    main()
