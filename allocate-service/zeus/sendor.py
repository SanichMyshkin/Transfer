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

    log.info(f"Найдено chatId в GitLab: {len(all_chat_ids)}")
    return all_chat_ids


def main():
    get_chat_counts_since(SINCE)

    gl = gl_connect()
    get_gitlab_chat_ids(gl)


if __name__ == "__main__":
    main()
