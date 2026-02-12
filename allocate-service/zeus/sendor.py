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
CHAT_ID_IN_STR_RE = re.compile(r'"chat_id"\s*:\s*"(?P<id>-?\d+)"')
ALERT_ALIASES = ("chat_sre", "chat_gods")


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
        log.error(f"[{project}] {path} -> YAML SNIPPET (first 500 chars): {snippet}")

        healed = text.replace("\t", "  ")
        try:
            data = yaml.safe_load(healed)
            log.warning(f"[{project}] {path} -> YAML healed (TAB->spaces)")
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


def normalize_cipher_value(v, project, path):
    s = str(v or "").strip()
    if not s.startswith("{cipher}"):
        log.error(f"[{project}] {path} -> chatId без {{cipher}}: {s}")
        return ""
    s = s[len("{cipher}") :].strip()
    if not s:
        log.error(f"[{project}] {path} -> пустой hash")
        return ""
    if not HEX_RE.fullmatch(s):
        log.error(f"[{project}] {path} -> не hex значение: {s}")
        return ""
    return s.lower()


def deep_find_values_by_key(node, key):
    out = []
    if isinstance(node, dict):
        for k, v in node.items():
            if str(k) == key:
                out.append(v)
            out.extend(deep_find_values_by_key(v, key))
    elif isinstance(node, list):
        for x in node:
            out.extend(deep_find_values_by_key(x, key))
    return out


def extract_chat_ids_from_obj(obj):
    ids = set()

    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower() == "chat_id":
                vv = str(v).strip()
                if vv:
                    ids.add(vv)
            else:
                ids |= extract_chat_ids_from_obj(v)

    elif isinstance(obj, list):
        for x in obj:
            ids |= extract_chat_ids_from_obj(x)

    elif isinstance(obj, str):
        s = obj.strip()
        if not s:
            return ids
        try:
            parsed = json.loads(s)
            ids |= extract_chat_ids_from_obj(parsed)
            return ids
        except Exception:
            pass
        for m in CHAT_ID_IN_STR_RE.finditer(s):
            ids.add(m.group("id"))

    return ids


def extract_zeus_cipher_hashes(text, project, path):
    data = parse_yaml_with_heal(text, project, path)
    if not data:
        return set()

    try:
        test_telegram = data["zeus"]["monitoringProperties"]["vars"]["zeusmonitoring"][
            "custom"
        ]["testTelegram"]
    except Exception:
        log.error(f"[{project}] {path} -> неверная структура zeus YAML")
        return set()

    if not isinstance(test_telegram, list):
        log.error(f"[{project}] {path} -> testTelegram не список")
        return set()

    hashes = set()
    for item in test_telegram:
        if not isinstance(item, dict):
            continue
        if "chatId" not in item:
            continue
        h = normalize_cipher_value(item["chatId"], project, path)
        if h:
            hashes.add(h)
    return hashes


def extract_alertmanager_chat_ids(text, project, path):
    data = parse_yaml_with_heal(text, project, path)
    if not data:
        return set()

    ids = set()
    found_any_alias = False
    for alias in ALERT_ALIASES:
        vals = deep_find_values_by_key(data, alias)
        if not vals:
            continue
        found_any_alias = True
        for v in vals:
            before = len(ids)
            ids |= extract_chat_ids_from_obj(v)
            if len(ids) == before:
                pass

    if found_any_alias and not ids:
        log.warning(f"[{project}] {path} -> aliases found, but chat_id not extracted")

    return ids


def is_zeus_monitor_file(path):
    p = (path or "").lower().split("/")[-1]
    return p.endswith("-monitors.yml") or p.endswith("-monitors.yaml")


def is_alertmanager_file(path):
    name = (path or "").lower().split("/")[-1]
    return name in {"values.yml", "values.yaml", "alert.yml", "alert.yaml"}


def scan_gitlab(gl):
    group = gl.groups.get(GROUP_ID)
    projects = group.projects.list(all=True, include_subgroups=True)

    zeus_chat_to_projects = defaultdict(set)
    am_chat_to_projects = defaultdict(set)

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

            need_zeus = is_zeus_monitor_file(file_path)
            need_am = is_alertmanager_file(file_path)
            if not (need_zeus or need_am):
                continue

            try:
                f = proj.files.get(file_path=file_path, ref=GIT_REF)
                text = f.decode().decode("utf-8")
            except Exception as e:
                log.error(f"[{proj_name}] чтение {file_path}: {e}")
                continue

            if need_zeus:
                hashes = extract_zeus_cipher_hashes(text, proj_name, file_path)
                if not hashes:
                    log.info(f"[{proj_name}] {file_path} -> zeus: пусто")
                else:
                    real_ids = set()
                    for h in hashes:
                        real = decrypt_cipher_hash(h)
                        if real:
                            real_ids.add(real)
                            zeus_chat_to_projects[str(real).strip()].add(proj_name)
                    log.info(
                        f"[{proj_name}] {file_path} -> zeus: found={len(real_ids)} ids={sorted(real_ids)[:20]}"
                    )

            if need_am:
                ids = extract_alertmanager_chat_ids(text, proj_name, file_path)
                if ids:
                    for cid in ids:
                        am_chat_to_projects[str(cid).strip()].add(proj_name)
                    log.info(
                        f"[{proj_name}] {file_path} -> alertmanager: found={len(ids)} ids={sorted(ids)[:20]}"
                    )

    return zeus_chat_to_projects, am_chat_to_projects


def build_rows(chat_to_projects, db_counts):
    rows = []
    for chat_id, projects in chat_to_projects.items():
        cnt = int(db_counts.get(str(chat_id).strip(), 0))
        for proj_full in projects:
            team, code = parse_team_and_code(proj_full)
            rows.append((team, code, str(chat_id).strip(), cnt))
    rows.sort(key=lambda x: (x[0], x[1], x[2]))
    return rows


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
    ]

    ws1 = wb.create_sheet("zeus")
    ws1.append(headers)
    for r in zeus_rows:
        ws1.append(list(r))

    ws2 = wb.create_sheet("alertmanager")
    ws2.append(headers)
    for r in am_rows:
        ws2.append(list(r))

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

    zeus_rows = build_rows(zeus_map, db_counts)
    am_rows = build_rows(am_map, db_counts)

    write_excel(OUT_XLSX, zeus_rows, am_rows)


if __name__ == "__main__":
    main()
