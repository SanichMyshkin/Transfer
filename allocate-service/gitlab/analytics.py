import os
import time
import logging
from pathlib import Path

import urllib3
import gitlab
import pandas as pd
from dotenv import load_dotenv
import humanize
from openpyxl import Workbook

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
BK_FILE = os.getenv("BK_FILE", "bk_all_users.xlsx")

OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "gitlab_projects_bt.xlsx")

SLEEP_SEC = 0.02
SSL_VERIFY = False
MAX_PROJECTS = 200
LOG_EVERY = 25

TARGET_BUSINESS_TYPE = "пуп"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("gitlab_projects_bt")


def die(msg: str, code: int = 2):
    log.error(msg)
    raise SystemExit(code)


def normalize_login(x: str) -> str:
    return (x or "").strip().lower()


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def connect():
    if not GITLAB_URL:
        die("GITLAB_URL не задан")
    if not GITLAB_TOKEN:
        die("GITLAB_TOKEN не задан")
    log.info("Подключение к GitLab api...")
    gl = gitlab.Gitlab(
        GITLAB_URL,
        private_token=GITLAB_TOKEN,
        ssl_verify=SSL_VERIFY,
        timeout=60,
        per_page=100,
    )
    gl.auth()
    log.info("Подключение успешно")
    return gl


def load_bk_login_business_type_map(path: str):
    if not path or not os.path.isfile(path):
        die(f"BK_FILE не найден: {path}")

    LOGIN_COL_IDX = 55
    BUSINESS_TYPE_COL_IDX = 44

    df = pd.read_excel(path, dtype=str, engine="openpyxl", header=0).fillna("")

    sub = df.iloc[:, [LOGIN_COL_IDX, BUSINESS_TYPE_COL_IDX]].copy()
    sub.columns = ["login", "business_type"]

    sub["login_key"] = sub["login"].map(normalize_login)
    sub["business_type"] = sub["business_type"].astype(str).map(clean_spaces)

    sub = sub[sub["login_key"] != ""].drop_duplicates("login_key", keep="last")
    mp = dict(zip(sub["login_key"], sub["business_type"]))

    log.info(f"BK: загружено login → тип бизнеса: {len(mp)}")
    return mp


def resolve_business_type(project_name, creator_username, maintainers, bk_map):
    creator_key = normalize_login(creator_username)

    if creator_key:
        bt = bk_map.get(creator_key, "")
        if bt:
            log.info(f'BT project="{project_name}" source=creator bt="{bt}"')
            return bt
        else:
            log.info(f'BT project="{project_name}" creator="{creator_username}" not_found')

    found = []
    for u in maintainers:
        k = normalize_login(u)
        t = bk_map.get(k, "")
        if t:
            found.append((u, t))

    if not found:
        log.info(f'BT project="{project_name}" source=none bt=""')
        return ""

    counts = {}
    for _, t in found:
        counts[t] = counts.get(t, 0) + 1

    max_cnt = max(counts.values())
    winners = [k for k, v in counts.items() if v == max_cnt]
    winners.sort()
    bt = winners[0]

    log.info(f'BT project="{project_name}" source=maintainers chosen="{bt}"')
    return bt


def main():
    log.info("Старт отчета по одному типу бизнеса")
    gl = connect()
    bk_map = load_bk_login_business_type_map(BK_FILE)

    target = clean_spaces(TARGET_BUSINESS_TYPE)

    out_path = str(Path(OUTPUT_XLSX).resolve())
    wb = Workbook()
    ws = wb.active
    ws.title = "Projects"
    ws.append(
        [
            "project",
            "creator",
            "maintainers",
            "bt_resolved",
            "bt_source",
            "repo_size",
            "job_artifacts_size",
            "total_size",
        ]
    )

    user_cache = {}
    errors = 0
    matched = 0
    start_ts = time.time()

    log.info(f'Фильтр: TARGET_BUSINESS_TYPE="{target}"')
    log.info(f"Начинаем обход проектов (limit={MAX_PROJECTS})")

    for i, p in enumerate(gl.projects.list(all=True, iterator=True), start=1):
        if i > MAX_PROJECTS:
            break

        proj_id = getattr(p, "id", None)
        proj_name = getattr(p, "path_with_namespace", None) or getattr(p, "name", None) or str(proj_id)

        try:
            full = gl.projects.get(proj_id, statistics=True)

            creator_username = ""
            creator_id = getattr(full, "creator_id", None)
            if creator_id:
                if creator_id in user_cache:
                    creator_username = user_cache[creator_id]
                else:
                    u = gl.users.get(creator_id)
                    creator_username = (getattr(u, "username", "") or "").strip()
                    user_cache[creator_id] = creator_username

            maintainers = []
            try:
                members = full.members_all.list(all=True)
            except Exception:
                members = full.members.list(all=True)

            for m in members:
                lvl = int(getattr(m, "access_level", 0) or 0)
                if lvl >= 40:
                    uname = (getattr(m, "username", "") or "").strip()
                    if uname:
                        maintainers.append(uname)

            maintainers_unique = sorted(set(maintainers))
            maintainers_str = ",".join(maintainers_unique)

            creator_key = normalize_login(creator_username)
            bt = ""
            bt_source = "none"

            if creator_key:
                bt = bk_map.get(creator_key, "")
                if bt:
                    bt_source = "creator"
                else:
                    bt = ""

            if not bt:
                found = []
                for u in maintainers_unique:
                    k = normalize_login(u)
                    t = bk_map.get(k, "")
                    if t:
                        found.append(t)

                if found:
                    counts = {}
                    for t in found:
                        counts[t] = counts.get(t, 0) + 1
                    max_cnt = max(counts.values())
                    winners = [k for k, v in counts.items() if v == max_cnt]
                    winners.sort()
                    bt = winners[0]
                    bt_source = "maintainers"

            bt = clean_spaces(bt)

            if bt != target:
                continue

            stats = getattr(full, "statistics", {}) or {}
            repo_bytes = int(stats.get("repository_size", 0) or 0)
            job_bytes = int(stats.get("job_artifacts_size", 0) or 0)
            total_bytes = repo_bytes + job_bytes

            repo_h = humanize.naturalsize(repo_bytes, binary=True)
            job_h = humanize.naturalsize(job_bytes, binary=True) if job_bytes else ""
            total_h = humanize.naturalsize(total_bytes, binary=True)

            log.info(
                f'INCLUDE project="{proj_name}" bt="{bt}" source={bt_source} '
                f'creator="{creator_username}" maintainers="{maintainers_str}" '
                f'repo="{repo_h}" job="{job_h or 0}" total="{total_h}"'
            )

            ws.append(
                [
                    proj_name,
                    creator_username,
                    maintainers_str,
                    bt,
                    bt_source,
                    repo_h,
                    job_h,
                    total_h,
                ]
            )
            matched += 1

        except Exception as e:
            errors += 1
            log.warning(f'FAIL project="{proj_name}" err={e}')

        if LOG_EVERY and i % LOG_EVERY == 0:
            elapsed = time.time() - start_ts
            rate = i / elapsed if elapsed > 0 else 0
            log.info(f"PROGRESS i={i} rate={rate:.2f} proj/s matched={matched} errors={errors}")

        if SLEEP_SEC > 0:
            time.sleep(SLEEP_SEC)

    wb.save(out_path)
    log.info(f"Saved: {out_path} | rows={ws.max_row - 1} matched={matched} errors={errors}")


if __name__ == "__main__":
    main()
