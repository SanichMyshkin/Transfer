# add_service_id_topic.py

import os
import time
import logging
import urllib3
import pandas as pd
import gitlab
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

INPUT_XLSX = "gitlab_test.xlsx"
PROJECT_ID_COL_IDX = 0
SERVICE_ID_COL_IDX = 1

DRY_RUN = True
SSL_VERIFY = False
LIMIT = 0
SLEEP_SEC = 0.1

TOPIC_PREFIX = "service_id:"

GITLAB_URL = (os.getenv("GITLAB_URL") or "").rstrip("/")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN") or ""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("gitlab_service_topic")


def die(msg):
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


def main():
    gl = connect()

    if not os.path.isfile(INPUT_XLSX):
        die(f"Файл не найден: {INPUT_XLSX}")

    df = pd.read_excel(INPUT_XLSX, dtype=str, engine="openpyxl").fillna("")
    log.info(f"Строк в файле: {len(df)}")

    tasks = []

    for i in range(len(df)):
        raw_id = str(df.iat[i, PROJECT_ID_COL_IDX]).strip()
        raw_service = str(df.iat[i, SERVICE_ID_COL_IDX]).strip()

        if not raw_id or not raw_service:
            continue

        try:
            proj_id = int(float(raw_id))
            service_id = str(int(float(raw_service)))
        except Exception:
            log.warning(f"SKIP_ROW idx={i+2} bad_data")
            continue

        tasks.append((proj_id, service_id))

    if LIMIT > 0:
        tasks = tasks[:LIMIT]
        log.info(f"LIMIT включён: {len(tasks)} строк")

    log.info(f"DRY_RUN={DRY_RUN}")

    for proj_id, service_id in tasks:
        try:
            p = gl.projects.get(proj_id)
            name = getattr(p, "path_with_namespace", str(proj_id))

            current = list(getattr(p, "topics", []) or [])

            # проверяем, есть ли уже service_id:*
            existing_service_topics = [
                t for t in current if t.startswith(TOPIC_PREFIX)
            ]

            if existing_service_topics:
                log.info(
                    f'SKIP_ALREADY_HAS_SERVICE project_id={proj_id} '
                    f'project="{name}" existing={existing_service_topics}'
                )
                continue

            new_topic = f"{TOPIC_PREFIX}{service_id}"
            final_topics = current + [new_topic]

            if DRY_RUN:
                log.info(
                    f'DRY_RUN_ADD project_id={proj_id} '
                    f'project="{name}" add="{new_topic}"'
                )
            else:
                p.topics = final_topics
                p.save()
                log.info(
                    f'APPLIED project_id={proj_id} '
                    f'project="{name}" add="{new_topic}"'
                )

        except gitlab.exceptions.GitlabGetError as e:
            log.warning(f"SKIP_MISSING project_id={proj_id} err={e}")
        except Exception as e:
            log.error(f"FAIL project_id={proj_id} err={e}")

        if SLEEP_SEC:
            time.sleep(SLEEP_SEC)

    log.info("Готово")


if __name__ == "__main__":
    main()
