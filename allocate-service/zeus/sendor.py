import os
import logging
from typing import Any, Dict, Iterable, List, Set, Tuple

from dotenv import load_dotenv
import psycopg2
import gitlab
import yaml

load_dotenv()

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


def get_chat_counts_since(since: str) -> List[Tuple[int, int]]:
    sql = """
        select chat_id, count(*)
        from sender.telegram_events_history
        where created >= %s
        group by chat_id
        order by count(*) desc
    """

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = False
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ")

    try:
        with conn:
            with conn.cursor() as cur:
                log.info(f"DB: подключено к {DB_HOST}:{DB_PORT}/{DB_NAME}")
                cur.execute(sql, (since,))
                rows = cur.fetchall()
                log.info(f"DB: получено строк {len(rows)}")
                return rows
    finally:
        conn.close()
        log.info("DB: соединение закрыто")


def gl_connect() -> gitlab.Gitlab:
    log.info("GitLab: подключаемся...")
    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN, ssl_verify=False, timeout=60)
    gl.auth()
    log.info("GitLab: ok")
    return gl


def get_group_projects(gl: gitlab.Gitlab):
    log.info(
        f"GitLab: получаем проекты группы GROUP_ID={GROUP_ID} (включая сабгруппы)..."
    )
    group = gl.groups.get(GROUP_ID)
    projs = group.projects.list(all=True, include_subgroups=True)
    log.info(f"GitLab: проектов найдено: {len(projs)}")
    return projs


def repo_tree(proj, path: str | None = None):
    if path:
        return proj.repository_tree(path=path, all=True)
    return proj.repository_tree(all=True)


def get_file_text(proj, file_path: str, ref: str) -> str:
    f = proj.files.get(file_path=file_path, ref=ref)
    return f.decode().decode("utf-8")


def find_zeus_dirs(root_items: List[Dict[str, Any]]) -> List[str]:
    return [
        i["path"]
        for i in root_items
        if i.get("type") == "tree" and (i.get("name") or "").startswith("zeus-")
    ]


def find_monitor_files_in_zeus(proj) -> List[str]:
    try:
        root = repo_tree(proj)
    except Exception as e:
        log.warning(f"[{proj.path_with_namespace}] repository_tree error: {e}")
        return []

    zeus_dirs = find_zeus_dirs(root)
    if not zeus_dirs:
        return []

    files: List[str] = []
    for zeus_dir in zeus_dirs:
        try:
            zeus_items = repo_tree(proj, zeus_dir)
        except Exception as e:
            log.warning(f"[{proj.path_with_namespace}] tree({zeus_dir}) error: {e}")
            continue

        subfolders = [i for i in zeus_items if i.get("type") == "tree"]

        for sub in subfolders:
            sub_path = sub["path"]
            try:
                sub_items = repo_tree(proj, sub_path)
            except Exception as e:
                log.warning(f"[{proj.path_with_namespace}] tree({sub_path}) error: {e}")
                continue

            for f in sub_items:
                if f.get("type") != "blob":
                    continue
                name = (f.get("name") or "").lower()
                # как ты просил: именно *-monitors.yml/yaml
                if name.endswith("-monitors.yml") or name.endswith("-monitors.yaml"):
                    files.append(f["path"])

    return files


def iter_chat_ids_from_obj(obj: Any) -> Iterable[int]:
    if obj is None:
        return

    if isinstance(obj, dict):
        for k in ("chatId", "chat_id", "chatID", "chatid"):
            if k in obj:
                v = obj.get(k)
                for x in iter_chat_ids_from_obj(v):
                    yield x

        for v in obj.values():
            for x in iter_chat_ids_from_obj(v):
                yield x

    elif isinstance(obj, list):
        for item in obj:
            for x in iter_chat_ids_from_obj(item):
                yield x

    elif isinstance(obj, int):
        yield obj

    elif isinstance(obj, str):
        s = obj.strip()
        # иногда могут быть кавычки/пробелы
        if s.lstrip("-").isdigit():
            try:
                yield int(s)
            except Exception:
                return


def extract_chat_ids_from_yaml_text(text: str) -> Set[int]:
    try:
        data = yaml.safe_load(text)
    except Exception:
        return set()

    chat_ids: Set[int] = set()
    for cid in iter_chat_ids_from_obj(data):
        chat_ids.add(cid)
    return chat_ids


def get_cipher_chats(gl: gitlab.Gitlab) -> Dict[int, List[str]]:
    projs = get_group_projects(gl)

    chat_sources: Dict[int, List[str]] = {}

    for p in projs:
        try:
            proj = gl.projects.get(p.id)
        except Exception as e:
            log.warning(
                f"[{getattr(p, 'path_with_namespace', p.id)}] project.get error: {e}"
            )
            continue

        files = find_monitor_files_in_zeus(proj)
        if not files:
            continue

        for fp in files:
            try:
                txt = get_file_text(proj, fp, GIT_REF)
            except Exception as e:
                log.warning(f"[{proj.path_with_namespace}] read {fp} error: {e}")
                continue

            cids = extract_chat_ids_from_yaml_text(txt)
            if not cids:
                continue

            src = f"{proj.path_with_namespace}:{fp}"
            for cid in cids:
                chat_sources.setdefault(cid, []).append(src)

    log.info(f"GitLab: уникальных chat_id в monitors-файлах: {len(chat_sources)}")
    return chat_sources


def main():
    db_counts = get_chat_counts_since(SINCE)
    db_chat_ids = {cid for cid, _ in db_counts}

    gl = gl_connect()
    cfg_sources = get_cipher_chats(gl)
    cfg_chat_ids = set(cfg_sources.keys())

    only_in_db = sorted(db_chat_ids - cfg_chat_ids)
    only_in_cfg = sorted(cfg_chat_ids - db_chat_ids)
    in_both = sorted(db_chat_ids & cfg_chat_ids)

    log.info(f"RESULT: chat_id только в БД (после {SINCE}): {len(only_in_db)}")
    log.info(f"RESULT: chat_id только в конфигах: {len(only_in_cfg)}")
    log.info(f"RESULT: chat_id и в БД и в конфигах: {len(in_both)}")

    log.info("TOP DB chat_id by count:")
    for cid, cnt in db_counts[:20]:
        tag = []
        if cid in cfg_sources:
            tag.append("in_cfg")
        log.info(f"  {cid}: {cnt}" + (f" ({', '.join(tag)})" if tag else ""))

    if in_both:
        sample = in_both[:20]
        log.info("SOURCES for first chats that are in both:")
        for cid in sample:
            srcs = cfg_sources.get(cid, [])
            log.info(f"  {cid}:")
            for s in srcs[:10]:
                log.info(f"    - {s}")


if __name__ == "__main__":
    main()
