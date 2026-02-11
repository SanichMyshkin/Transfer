import os
import logging
from dotenv import load_dotenv
import psycopg2
import gitlab
import yaml
import urllib3

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

SINCE = os.getenv("SINCE", "2026-01-01 00:00:00")

GITLAB_URL = os.getenv("GITLAB_URL", "").rstrip("/")
TOKEN = os.getenv("TOKEN", "")
GROUP_ID = os.getenv("GROUP_ID", "").strip()
GIT_REF = "main"


def get_chat_counts_since(since):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.set_session(readonly=True)

    try:
        with conn:
            with conn.cursor() as cur:
                log.info("DB подключено")
                cur.execute(
                    """
                    select chat_id, count(*)
                    from sender.telegram_events_history
                    where created >= %s
                    group by chat_id
                    """,
                    (since,),
                )
                rows = cur.fetchall()
                log.info(f"Получено {len(rows)} chat_id из БД")
                return rows
    finally:
        conn.close()


def gl_connect():
    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN, ssl_verify=False)
    gl.auth()
    return gl


def extract_chat_ids_from_yaml(text):
    data = yaml.safe_load(text)
    result = set()

    if not data:
        return result

    def walk(obj):
        if isinstance(obj, dict):
            if "chatId" in obj:
                v = obj["chatId"]
                if isinstance(v, str) and v.lstrip("-").isdigit():
                    result.add(int(v))
                elif isinstance(v, int):
                    result.add(v)
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(data)
    return result


def get_gitlab_chat_ids(gl):
    group = gl.groups.get(GROUP_ID)
    projects = group.projects.list(all=True, include_subgroups=True)

    all_chat_ids = set()

    for p in projects:
        proj = gl.projects.get(p.id)
        log.info(f"Обрабатываем проект: {proj.path_with_namespace}")

        try:
            tree = proj.repository_tree(all=True)
        except Exception:
            continue

        for item in tree:
            if item["type"] != "blob":
                continue

            name = item["name"].lower()
            if not (name.endswith("-monitors.yml") or name.endswith("-monitors.yaml")):
                continue

            try:
                f = proj.files.get(file_path=item["path"], ref=GIT_REF)
                text = f.decode().decode("utf-8")
                chat_ids = extract_chat_ids_from_yaml(text)
                all_chat_ids.update(chat_ids)
            except Exception:
                continue

    log.info(f"Уникальных chat_id в GitLab: {len(all_chat_ids)}")
    return all_chat_ids


def main():
    db_rows = get_chat_counts_since(SINCE)
    db_chat_ids = {cid for cid, _ in db_rows}

    gl = gl_connect()
    gitlab_chat_ids = get_gitlab_chat_ids(gl)

    only_in_db = db_chat_ids - gitlab_chat_ids
    only_in_git = gitlab_chat_ids - db_chat_ids
    in_both = db_chat_ids & gitlab_chat_ids

    log.info(f"Только в БД: {len(only_in_db)}")
    log.info(f"Только в GitLab: {len(only_in_git)}")
    log.info(f"И там и там: {len(in_both)}")


if __name__ == "__main__":
    main()
