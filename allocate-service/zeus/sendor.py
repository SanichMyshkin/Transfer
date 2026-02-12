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
GIT_REF = os.getenv("GIT_REF", "main")


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
    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN, ssl_verify=False, timeout=60)
    gl.auth()
    return gl


def normalize_cipher_value(v):
    s = str(v or "").strip()
    if not s.startswith("{cipher}"):
        return ""
    s = s[len("{cipher}") :].strip()
    if not s:
        return ""
    hex_only = True
    for ch in s:
        if ch not in "0123456789abcdefABCDEF":
            hex_only = False
            break
    if not hex_only:
        return ""
    return s.lower()


def extract_cipher_hashes_from_yaml(text):
    try:
        data = yaml.safe_load(text)
    except Exception:
        return set()
    if not data:
        return set()

    try:
        test_telegram = (
            data["zeus"]["monitoringProperties"]["vars"]["zeusmonitoring"]["custom"].get(
                "testTelegram"
            )
        )
    except Exception:
        return set()

    result = set()
    if isinstance(test_telegram, list):
        for item in test_telegram:
            if isinstance(item, dict) and "chatId" in item:
                h = normalize_cipher_value(item.get("chatId"))
                if h:
                    result.add(h)
    return result


def get_gitlab_cipher_map(gl):
    group = gl.groups.get(GROUP_ID)
    projects = group.projects.list(all=True, include_subgroups=True)

    cipher_map = {}

    for p in projects:
        proj = gl.projects.get(p.id)
        log.info(f"Обрабатываем проект: {proj.path_with_namespace}")

        try:
            tree = proj.repository_tree(ref=GIT_REF, recursive=True, all=True)
        except Exception as e:
            log.warning(f"[{proj.path_with_namespace}] repository_tree error: {e}")
            continue

        found_files = 0

        for item in tree:
            if item.get("type") != "blob":
                continue

            name = (item.get("name") or "").lower()
            if not (name.endswith("-monitors.yml") or name.endswith("-monitors.yaml")):
                continue

            found_files += 1

            try:
                f = proj.files.get(file_path=item["path"], ref=GIT_REF)
                text = f.decode().decode("utf-8")
                hashes = extract_cipher_hashes_from_yaml(text)
                if hashes:
                    log.info(
                        f"[{proj.path_with_namespace}] {item['path']} -> {len(hashes)}"
                    )
                for h in hashes:
                    cipher_map.setdefault(h, set()).add(proj.path_with_namespace)
            except Exception as e:
                log.warning(
                    f"[{proj.path_with_namespace}] read {item['path']} error: {e}"
                )
                continue

        log.info(f"[{proj.path_with_namespace}] monitors-файлов найдено: {found_files}")

    out = {}
    for h, projs in cipher_map.items():
        out[h] = sorted(projs)
    return out


def main():
    get_chat_counts_since(SINCE)
    gl = gl_connect()
    cipher_map = get_gitlab_cipher_map(gl)

    for h in sorted(cipher_map.keys()):
        print(h)
        for p in cipher_map[h]:
            print(f"  {p}")


if __name__ == "__main__":
    main()
