import os
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import psycopg2
import requests
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

SINCE_DAYS = int(os.getenv("SINCE", "90"))

GITLAB_URL = os.getenv("GITLAB_URL", "").rstrip("/")
TOKEN = os.getenv("TOKEN", "")
GROUP_ID = os.getenv("GROUP_ID", "").strip()
GIT_REF = os.getenv("GIT_REF", "main")

DECRYPT_URL = os.getenv("DECRYPT_URL")

HTTP_TIMEOUT_SEC = 30


def gl_connect():
    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN, ssl_verify=False, timeout=60)
    gl.auth()
    return gl


def get_chat_counts_since_days(days: int):
    since_dt = datetime.now(timezone.utc) - timedelta(days=days)
    since_str = since_dt.strftime("%Y-%m-%d %H:%M:%S%z")

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
                log.info(f"DB: подключено, since_days={days}")
                cur.execute(
                    """
                    select chat_id, count(*)
                    from sender.telegram_events_history
                    where created >= %s
                    group by chat_id
                    """,
                    (since_dt,),
                )
                rows = cur.fetchall()
                log.info(f"DB: получено строк {len(rows)}")
                return rows, since_str
    finally:
        conn.close()
        log.info("DB: соединение закрыто")


def normalize_cipher_value(v, project, path):
    s = str(v or "").strip()
    if not s.startswith("{cipher}"):
        log.error(f"[{project}] {path} -> chatId без {{cipher}}: {s}")
        return ""
    s = s[len("{cipher}") :].strip()
    if not s:
        log.error(f"[{project}] {path} -> пустой hash")
        return ""
    for ch in s:
        if ch not in "0123456789abcdefABCDEF":
            log.error(f"[{project}] {path} -> не hex значение: {s}")
            return ""
    return s.lower()


def parse_yaml_with_heal(text, project, path):
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as e:
        log.warning(f"[{project}] {path} -> YAML parse error: {e}")
        log.warning(f"[{project}] {path} -> пытаемся вылечить (TAB -> пробелы)")
        healed = text.replace("\t", "  ")
        try:
            data = yaml.safe_load(healed)
            log.warning(f"[{project}] {path} -> вылечили YAML")
            return data
        except yaml.YAMLError as e2:
            log.error(f"[{project}] {path} -> не смогли вылечить YAML: {e2}")
            return None
    except Exception as e:
        log.error(f"[{project}] {path} -> YAML parse error: {e}")
        return None


def extract_cipher_hashes_from_yaml(text, project, path):
    data = parse_yaml_with_heal(text, project, path)
    if not data:
        return set()

    try:
        test_telegram = data["zeus"]["monitoringProperties"]["vars"]["zeusmonitoring"][
            "custom"
        ]["testTelegram"]
    except Exception:
        log.error(f"[{project}] неправильная структура YAML в {path}")
        return set()

    if not isinstance(test_telegram, list):
        log.error(f"[{project}] testTelegram не список в {path}")
        return set()

    result = set()
    for item in test_telegram:
        if not isinstance(item, dict):
            log.error(f"[{project}] testTelegram элемент не dict в {path}")
            continue
        if "chatId" not in item:
            log.error(f"[{project}] нет chatId в {path}")
            continue
        h = normalize_cipher_value(item["chatId"], project, path)
        if h:
            result.add(h)
    return result


def get_gitlab_cipher_map(gl):
    group = gl.groups.get(GROUP_ID)
    projects = group.projects.list(all=True, include_subgroups=True)

    cipher_map = {}  # cipher_hash -> set(projects)

    for p in projects:
        proj = gl.projects.get(p.id)
        log.info(f"GitLab: проект {proj.path_with_namespace}")

        try:
            tree = proj.repository_tree(ref=GIT_REF, recursive=True, all=True)
        except Exception as e:
            log.error(f"[{proj.path_with_namespace}] repository_tree error: {e}")
            continue

        for item in tree:
            if item.get("type") != "blob":
                continue

            name = (item.get("name") or "").lower()
            if not (name.endswith("-monitors.yml") or name.endswith("-monitors.yaml")):
                continue

            try:
                f = proj.files.get(file_path=item["path"], ref=GIT_REF)
                text = f.decode().decode("utf-8")
                hashes = extract_cipher_hashes_from_yaml(
                    text, proj.path_with_namespace, item["path"]
                )
                for h in hashes:
                    cipher_map.setdefault(h, set()).add(proj.path_with_namespace)
            except Exception as e:
                log.error(f"[{proj.path_with_namespace}] чтение {item['path']}: {e}")

    return {h: sorted(v) for h, v in cipher_map.items()}


def decrypt_cipher_hash(cipher_hash: str):
    r = requests.post(
        url=DECRYPT_URL,
        data=cipher_hash,
        headers={"Content-Type": "text/plain"},
        verify=False,
        timeout=HTTP_TIMEOUT_SEC,
    )
    if r.status_code != 200:
        log.error(f"DECRYPT: {cipher_hash} -> HTTP {r.status_code}: {r.text}")
        return ""
    return (r.text or "").strip()


def normalize_git_cipher_map(cipher_map: dict):
    """
    returns:
      chat_id(str) -> [projects]
    """
    result = {}
    for cipher_hash, projects in cipher_map.items():
        real_chat_id = decrypt_cipher_hash(cipher_hash)
        if not real_chat_id:
            continue
        if real_chat_id not in result:
            result[real_chat_id] = []
        result[real_chat_id].extend(projects)

    for k in list(result.keys()):
        result[k] = sorted(set(result[k]))
    return result


def build_db_count_map(db_rows):
    """
    returns:
      chat_id(str) -> count(int)
    """
    m = {}
    for chat_id, cnt in db_rows:
        key = str(chat_id).strip()
        if not key:
            continue
        m[key] = int(cnt or 0)
    return m


def build_project_rows(chat_to_projects: dict, db_counts: dict):
    """
    returns list of tuples:
      (project, chat_id, message_count)
    """
    rows = []
    for chat_id, projects in chat_to_projects.items():
        cnt = int(db_counts.get(str(chat_id).strip(), 0))
        for proj in projects:
            rows.append((proj, str(chat_id).strip(), cnt))

    rows.sort(key=lambda x: (x[0], x[1]))
    return rows


def main():
    db_rows, since_str = get_chat_counts_since_days(SINCE_DAYS)
    db_counts = build_db_count_map(db_rows)

    gl = gl_connect()
    cipher_map = get_gitlab_cipher_map(gl)
    chat_to_projects = normalize_git_cipher_map(cipher_map)

    project_rows = build_project_rows(chat_to_projects, db_counts)

    print(
        f"\n=== Итог по проектам (последние {SINCE_DAYS} дней, since={since_str}) ==="
    )
    print("project | chat_id | messages_count")
    for proj, chat_id, cnt in project_rows:
        print(f"{proj} | {chat_id} | {cnt}")

    git_chat_ids = set(chat_to_projects.keys())
    db_chat_ids = set(db_counts.keys())

    missing_in_db = sorted(git_chat_ids - db_chat_ids)
    missing_in_git = sorted(db_chat_ids - git_chat_ids)

    print("\n=== Диагностика ===")
    print(f"chat_id в Git (после decrypt): {len(git_chat_ids)}")
    print(f"chat_id в DB: {len(db_chat_ids)}")
    print(f"chat_id есть в Git, но нет в DB: {len(missing_in_db)}")
    for x in missing_in_db[:50]:
        print(f"  {x}")
    if len(missing_in_db) > 50:
        print(f"  ... и еще {len(missing_in_db) - 50}")

    print(f"\nchat_id есть в DB, но нет в Git: {len(missing_in_git)}")
    for x in missing_in_git[:50]:
        print(f"  {x}")
    if len(missing_in_git) > 50:
        print(f"  ... и еще {len(missing_in_git) - 50}")


if __name__ == "__main__":
    main()
