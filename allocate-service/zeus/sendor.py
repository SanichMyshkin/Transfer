import os
import re
import json
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict

from dotenv import load_dotenv
import psycopg2
import requests
import gitlab
import yaml
import urllib3

from openpyxl import Workbook

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

OUT_XLSX = os.getenv("OUT_XLSX", "chat_project_report.xlsx")
HTTP_TIMEOUT_SEC = 30

PROJECT_CODE_RE = re.compile(r"^(?P<team>.+)-(?P<code>\d+)$")
HEX_RE = re.compile(r"^[0-9a-fA-F]+$")
CHAT_ID_NUM_RE = re.compile(r"^-?\d+$")


def gl_connect():
    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN, ssl_verify=False, timeout=60)
    gl.auth()
    return gl


def get_chat_counts_since_days(days):
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


def build_db_count_map(db_rows):
    m = {}
    for chat_id, cnt in db_rows:
        key = str(chat_id).strip()
        if key:
            m[key] = int(cnt or 0)
    return m


def decrypt_cipher_hash(cipher_hash):
    if not DECRYPT_URL:
        return ""
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


def parse_yaml_with_heal(text, project, path):
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as e:
        log.error(f"[{project}] {path} -> YAML PARSE ERROR: {type(e).__name__}: {e}")
        snippet = text[:500].replace("\n", "\\n")
        log.error(f"[{project}] {path} -> YAML SNIPPET: {snippet}")

        healed = text.replace("\t", "  ")
        try:
            data = yaml.safe_load(healed)
            log.warning(f"[{project}] {path} -> YAML healed")
            return data
        except yaml.YAMLError as e2:
            log.error(
                f"[{project}] {path} -> YAML HEAL FAILED: {type(e2).__name__}: {e2}"
            )
            return None
    except Exception as e:
        log.error(f"[{project}] {path} -> YAML UNKNOWN ERROR: {type(e).__name__}: {e}")
        return None


def parse_team_and_code(project_path_with_namespace):
    last = (project_path_with_namespace or "").split("/")[-1].strip()
    if not last:
        return "", ""
    m = PROJECT_CODE_RE.match(last)
    if not m:
        return last, ""
    return (m.group("team") or "").strip(), (m.group("code") or "").strip()


def normalize_chat_id(raw):
    s = str(raw or "").strip()
    if not s:
        return ""

    if not CHAT_ID_NUM_RE.fullmatch(s):
        return ""

    core = s[1:] if s.startswith("-") else s
    if core and set(core) == {"0"}:
        return ""

    return s


def extract_chat_ids(text, project, path):
    data = parse_yaml_with_heal(text, project, path)
    if not data:
        return set()

    ids = set()

    def add_id(val):
        val = str(val or "").strip()
        if not val:
            return

        if val.startswith("{cipher}"):
            cipher = val[len("{cipher}") :].strip()
            if not HEX_RE.fullmatch(cipher):
                log.error(f"[{project}] {path} -> invalid cipher: {val}")
                return
            real = decrypt_cipher_hash(cipher)
            real_id = normalize_chat_id(real)
            if real_id:
                ids.add(real_id)
            return

        real_id = normalize_chat_id(val)
        if real_id:
            ids.add(real_id)

    def walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                kk = str(k).strip().lower()
                if kk in {"chat_id", "chatid"}:
                    add_id(v)
                walk(v)
        elif isinstance(node, list):
            for x in node:
                walk(x)
        elif isinstance(node, str):
            try:
                parsed = json.loads(node)
                walk(parsed)
            except Exception:
                pass

    walk(data)
    return ids


def path_has_dir_startswith(parts, prefix):
    for p in parts:
        if p.startswith(prefix):
            return True
    return False


def path_has_dir_equals(parts, name):
    for p in parts:
        if p == name:
            return True
    return False


def scan_gitlab(gl):
    group = gl.groups.get(GROUP_ID)
    projects = group.projects.list(all=True, include_subgroups=True)

    zeus_map = defaultdict(set)
    am_map = defaultdict(set)

    for p in projects:
        proj = gl.projects.get(p.id)
        proj_name = proj.path_with_namespace
        log.info(f"GitLab: проект {proj_name}")

        try:
            tree = proj.repository_tree(ref=GIT_REF, recursive=True, all=True)
        except Exception as e:
            log.error(f"[{proj_name}] repository_tree error: {e}")
            continue

        for item in tree:
            if item.get("type") != "blob":
                continue

            file_path = item.get("path") or ""
            if not file_path:
                continue

            path_lower = file_path.lower()
            parts = path_lower.split("/")
            name = parts[-1]

            need_zeus = path_has_dir_startswith(parts[:-1], "zeus") and (
                name.endswith("-monitors.yml") or name.endswith("-monitors.yaml")
            )

            need_am = path_has_dir_equals(parts[:-1], "monitoring") and (
                name in {"values.yml", "values.yaml", "alert.yml", "alert.yaml"}
            )

            if not (need_zeus or need_am):
                continue

            try:
                f = proj.files.get(file_path=file_path, ref=GIT_REF)
                text = f.decode().decode("utf-8")
            except Exception as e:
                log.error(f"[{proj_name}] чтение {file_path}: {e}")
                continue

            ids = extract_chat_ids(text, proj_name, file_path)
            if not ids:
                continue

            if need_zeus:
                for cid in ids:
                    zeus_map[cid].add(proj_name)
                log.info(
                    f"[{proj_name}] {file_path} -> zeus found={len(ids)} ids={sorted(ids)[:20]}"
                )

            if need_am:
                for cid in ids:
                    am_map[cid].add(proj_name)
                log.info(
                    f"[{proj_name}] {file_path} -> alertmanager found={len(ids)} ids={sorted(ids)[:20]}"
                )

    return zeus_map, am_map


def build_rows(chat_map, db_counts):
    rows = []
    total = 0

    for chat_id, projects in chat_map.items():
        cnt = int(db_counts.get(str(chat_id).strip(), 0))
        total += cnt
        for proj in projects:
            team, code = parse_team_and_code(proj)
            rows.append([team, code, chat_id, cnt])

    for r in rows:
        cnt = int(r[3] or 0)
        pct = 0.0 if total <= 0 else (cnt * 100.0 / total)
        r.append(pct)

    rows.sort(key=lambda x: (x[0], x[1], x[2]))
    return rows, total


def log_chat_id_sets(name, git_ids, db_ids):
    git_sorted = sorted(git_ids)
    db_sorted = sorted(db_ids)
    log.info(f"[{name}] chat_id Git = {len(git_sorted)} sample={git_sorted[:50]}")
    log.info(f"[{name}] chat_id DB  = {len(db_sorted)} sample={db_sorted[:50]}")
    only_git = sorted(set(git_ids) - set(db_ids))
    only_db = sorted(set(db_ids) - set(git_ids))
    log.info(f"[{name}] Git-DB = {len(only_git)} sample={only_git[:50]}")
    log.info(f"[{name}] DB-Git = {len(only_db)} sample={only_db[:50]}")


def write_excel(out_path, zeus_rows, am_rows):
    wb = Workbook()
    wb.remove(wb.active)

    headers = [
        "Наименование команды",
        "Код (номер команды)",
        "Chat ID",
        "Кол-во сообщений",
        "% от общего",
    ]

    ws1 = wb.create_sheet("zeus")
    ws1.append(headers)
    for r in zeus_rows:
        ws1.append(r)

    ws2 = wb.create_sheet("alertmanager")
    ws2.append(headers)
    for r in am_rows:
        ws2.append(r)

    wb.save(out_path)
    log.info(f"XLSX: saved {out_path}")


def main():
    db_rows, since_str = get_chat_counts_since_days(SINCE_DAYS)
    db_counts = build_db_count_map(db_rows)
    db_chat_ids = set(db_counts.keys())

    gl = gl_connect()
    zeus_map, am_map = scan_gitlab(gl)

    log.info(f"since={since_str} days={SINCE_DAYS}")

    log_chat_id_sets("zeus", set(zeus_map.keys()), db_chat_ids)
    log_chat_id_sets("alertmanager", set(am_map.keys()), db_chat_ids)

    zeus_rows, zeus_total = build_rows(zeus_map, db_counts)
    am_rows, am_total = build_rows(am_map, db_counts)

    log.info(f"[zeus] total_messages={zeus_total}")
    log.info(f"[alertmanager] total_messages={am_total}")

    write_excel(OUT_XLSX, zeus_rows, am_rows)


if __name__ == "__main__":
    main()
