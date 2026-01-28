import os
import re
import logging
import urllib3

import gitlab
import yaml
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("test_gitlab_yaml")

load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL", "").rstrip("/")
TOKEN = os.getenv("TOKEN", "")
GROUP_ID = os.getenv("GROUP_ID", "")
GIT_REF = os.getenv("GIT_REF", "main")

TAB_REPLACEMENT = os.getenv("TAB_REPLACEMENT", "  ")
DROP_FULL_LINE_COMMENTS = os.getenv("DROP_FULL_LINE_COMMENTS", "true").strip().lower() in {
    "1", "true", "yes", "y", "on"
}


def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"Не задано окружение {name}")
    return v


def gl_connect():
    must_env("GITLAB_URL")
    must_env("TOKEN")
    must_env("GROUP_ID")
    log.info("Подключаемся к GitLab...")
    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN, ssl_verify=False, timeout=60)
    gl.auth()
    log.info("GitLab: ok")
    return gl


def repo_tree(proj, path=None):
    if path:
        return proj.repository_tree(path=path, all=True)
    return proj.repository_tree(all=True)


def get_file_text(proj, file_path: str, ref: str) -> str:
    f = proj.files.get(file_path=file_path, ref=ref)
    return f.decode().decode("utf-8")


def find_monitoring_files(proj):
    root = repo_tree(proj)
    zeus_dir = next(
        (i for i in root if i["type"] == "tree" and i["name"].startswith("zeus-")),
        None,
    )
    if not zeus_dir:
        return []

    files = []
    zeus_items = repo_tree(proj, zeus_dir["path"])
    subfolders = [i for i in zeus_items if i["type"] == "tree"]

    for sub in subfolders:
        sub_items = repo_tree(proj, sub["path"])
        for f in sub_items:
            if f["type"] != "blob":
                continue
            n = (f.get("name") or "").lower()
            if n.endswith("-monitors.yaml") or n.endswith("-monitors.yml"):
                files.append(f["path"])

    return files


def normalize_yaml_text(raw: str) -> str:
    txt = raw.replace("\t", TAB_REPLACEMENT) if "\t" in raw else raw
    if DROP_FULL_LINE_COMMENTS:
        out = []
        for line in txt.splitlines():
            if line.lstrip().startswith("#"):
                continue
            out.append(line)
        txt = "\n".join(out) + "\n"
    return txt


def extract_listing_block(text: str) -> str | None:
    lines = text.splitlines()
    listing_idx = None
    listing_indent = None

    for i, line in enumerate(lines):
        if re.match(r"^\s*listing\s*:\s*(#.*)?$", line):
            listing_idx = i
            listing_indent = len(line) - len(line.lstrip(" "))
            break

    if listing_idx is None:
        return None

    block = []
    for j in range(listing_idx + 1, len(lines)):
        ln = lines[j]
        if not ln.strip():
            block.append(ln)
            continue
        indent = len(ln) - len(ln.lstrip(" "))
        if indent <= listing_indent:
            break
        block.append(ln)

    return "\n".join(block) if block else None


def split_listing_items(block: str) -> list[str]:
    lines = block.splitlines()
    item_indent = None
    for ln in lines:
        m = re.match(r"^(\s*)-\s+", ln)
        if m:
            item_indent = len(m.group(1))
            break
    if item_indent is None:
        return []

    items = []
    cur = []
    pat = re.compile(r"^" + (" " * item_indent) + r"-\s+")
    for ln in lines:
        if pat.match(ln):
            if cur:
                items.append("\n".join(cur))
            cur = [ln]
        else:
            if cur:
                cur.append(ln)
    if cur:
        items.append("\n".join(cur))
    return items


def fallback_parse_listing(text: str):
    block = extract_listing_block(text)
    if not block:
        return []

    items = split_listing_items(block)
    if not items:
        return []

    out = []
    for it in items:
        m_en = re.search(r"(?m)^\s*enabled\s*:\s*(true|false)\s*$", it, re.IGNORECASE)
        enabled = None
        if m_en:
            enabled = m_en.group(1).lower() == "true"

        tg = bool(re.search(r"(?is)sendersStatus\s*:\s*.*?\btelegram\s*:\s*true\b", it))
        ml = bool(re.search(r"(?is)sendersStatus\s*:\s*.*?\bmail\s*:\s*true\b", it))

        out.append({"enabled": enabled, "has_notifications": tg or ml})

    return out


def main():
    gl = gl_connect()

    log.info(f"Получаем проекты группы GROUP_ID={GROUP_ID} ...")
    group = gl.groups.get(GROUP_ID)
    projects = group.projects.list(all=True, include_subgroups=True)
    log.info(f"Проектов найдено: {len(projects)}")
    if not projects:
        raise SystemExit("В группе нет проектов")

    # Берем первый проект
    p = projects[0]
    log.info(f"Берем первый проект: {p.name} (id={p.id})")
    proj = gl.projects.get(p.id)

    files = find_monitoring_files(proj)
    log.info(f"monitoring файлов найдено: {len(files)}")
    if not files:
        raise SystemExit("В первом проекте нет monitoring файлов")

    file_path = files[0]
    log.info(f"Берем первый monitoring файл: {file_path}")

    raw = get_file_text(proj, file_path, GIT_REF)
    log.info(f"Скачали файл: {len(raw)} bytes")

    text = normalize_yaml_text(raw)

    try:
        data = yaml.safe_load(text) or {}
        listing = (((data.get("zeus") or {}).get("monitors") or {}).get("listing")) or []
        log.info(f"PyYAML OK. listing элементов: {len(listing) if isinstance(listing, list) else 'not-a-list'}")
        if isinstance(listing, list) and listing:
            first = listing[0]
            enabled = first.get("enabled") if isinstance(first, dict) else None
            log.info(f"Первый элемент listing: enabled={enabled}")
        return
    except Exception as e:
        log.warning(f"PyYAML FAIL: {e}")

    monitors = fallback_parse_listing(text)
    log.info(f"FALLBACK: элементов listing найдено: {len(monitors)}")
    if monitors:
        log.info(f"FALLBACK first: enabled={monitors[0]['enabled']} notifications={monitors[0]['has_notifications']}")


if __name__ == "__main__":
    main()
