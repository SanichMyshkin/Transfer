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

OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "gitlab_projects.xlsx")

SLEEP_SEC = 0.02
SSL_VERIFY = False
MAX_PROJECTS = 50
LOG_EVERY = 25

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("gitlab_projects")


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

    gl = gitlab.Gitlab(
        GITLAB_URL,
        private_token=GITLAB_TOKEN,
        ssl_verify=SSL_VERIFY,
        timeout=60,
        per_page=100,
    )
    gl.auth()
    return gl


def load_bk_login_business_type_map(path: str):
    if not path or not os.path.isfile(path):
        die(f"BK_FILE не найден: {path}")

    LOGIN_COL_IDX = 55
    BUSINESS_TYPE_COL_IDX = 44

    df = pd.read_excel(path, dtype=str, engine="openpyxl", header=0).fillna("")

    max_idx = max(LOGIN_COL_IDX, BUSINESS_TYPE_COL_IDX)
    if df.shape[1] <= max_idx:
        die(
            f"BK_FILE: недостаточно колонок: cols={df.shape[1]}, "
            f"нужен минимум индекс {max_idx}"
        )

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
            log.info(
                f'BT project="{project_name}" source=creator login="{creator_username}" bt="{bt}"'
            )
            return bt
        else:
            log.info(
                f'BT project="{project_name}" creator="{creator_username}" not_found_in_bk'
            )

    found = []
    for u in maintainers:
        k = normalize_login(u)
        t = bk_map.get(k, "")
        if t:
            found.append((u, t))

    if not found:
        log.info(
            f'BT project="{project_name}" source=none creator="{creator_username}" '
            f'maintainers="{",".join(maintainers)}" bt=""'
        )
        return ""

    counts = {}
    for _, t in found:
        counts[t] = counts.get(t, 0) + 1

    max_cnt = max(counts.values())
    winners = [k for k, v in counts.items() if v == max_cnt]
    winners.sort()
    bt = winners[0]

    log.info(
        f'BT project="{project_name}" source=maintainers '
        f'found={found} counts={counts} chosen="{bt}"'
    )

    return bt


def main():
    gl = connect()
    bk_map = load_bk_login_business_type_map(BK_FILE)

    out_path = str(Path(OUTPUT_XLSX).resolve())
    wb = Workbook()
    ws = wb.active
    ws.title = "Projects"
    ws.append(["project", "creator", "maintainers", "business_type", "size"])

    user_cache = {}
    errors = 0
    start_ts = time.time()

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

            bt = resolve_business_type(
                proj_name, creator_username, maintainers_unique, bk_map
            )

            stats = getattr(full, "statistics", {}) or {}
            size_bytes = int(stats.get("repository_size", 0) or 0)
            size_human = humanize.naturalsize(size_bytes, binary=True)

            ws.append([proj_name, creator_username, maintainers_str, bt, size_human])

        except Exception as e:
            errors += 1
            ws.append([proj_name, "", "", "", f"ERROR: {e}"])
            log.warning(f'FAIL project="{proj_name}" err={e}')

        if LOG_EVERY and i % LOG_EVERY == 0:
            elapsed = time.time() - start_ts
            rate = i / elapsed if elapsed > 0 else 0
            log.info(f"PROGRESS i={i} rate={rate:.2f} proj/s errors={errors}")

        if SLEEP_SEC > 0:
            time.sleep(SLEEP_SEC)

    wb.save(out_path)
    log.info(f"Saved: {out_path} | rows={ws.max_row - 1} errors={errors}")


if __name__ == "__main__":
    main()
