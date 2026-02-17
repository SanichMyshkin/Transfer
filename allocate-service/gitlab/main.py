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

BAN_BUSINESS_TYPE = {
    "бан бизнес",
}

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
            return bt

    found = []
    for u in maintainers:
        k = normalize_login(u)
        t = bk_map.get(k, "")
        if t:
            found.append(t)

    if not found:
        return ""

    counts = {}
    for t in found:
        counts[t] = counts.get(t, 0) + 1

    max_cnt = max(counts.values())
    winners = [k for k, v in counts.items() if v == max_cnt]
    winners.sort()
    return winners[0]


def main():
    log.info("Старт отчета GitLab проектов")
    gl = connect()

    log.info("Получение данных из файла бк")
    bk_map = load_bk_login_business_type_map(BK_FILE)

    totals = {}
    user_cache = {}
    errors = 0
    start_ts = time.time()

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

            bt = resolve_business_type(
                proj_name, creator_username, maintainers_unique, bk_map
            )

            bt = clean_spaces(bt)

            if bt in BAN_BUSINESS_TYPE:
                log.info(f'PROJECT skip by banlist name="{proj_name}" bt="{bt}"')
                continue

            stats = getattr(full, "statistics", {}) or {}
            repo_bytes = int(stats.get("repository_size", 0) or 0)
            job_bytes = int(stats.get("job_artifacts_size", 0) or 0)

            if bt not in totals:
                totals[bt] = {"repo_bytes": 0, "job_bytes": 0}

            totals[bt]["repo_bytes"] += repo_bytes
            totals[bt]["job_bytes"] += job_bytes

        except Exception as e:
            errors += 1
            log.warning(f'FAIL project="{proj_name}" err={e}')

        if LOG_EVERY and i % LOG_EVERY == 0:
            elapsed = time.time() - start_ts
            rate = i / elapsed if elapsed > 0 else 0
            log.info(f"PROGRESS i={i} rate={rate:.2f} proj/s errors={errors}")

        if SLEEP_SEC > 0:
            time.sleep(SLEEP_SEC)

    out_path = str(Path(OUTPUT_XLSX).resolve())
    wb = Workbook()
    ws = wb.active
    ws.title = "BusinessTypes"
    ws.append(["business_type", "repo_size", "job_artifacts_size", "total_size", "percent_of_total"])

    grand_total = 0
    for v in totals.values():
        grand_total += int(v["repo_bytes"]) + int(v["job_bytes"])

    rows = []
    for bt, v in totals.items():
        repo_b = int(v["repo_bytes"])
        job_b = int(v["job_bytes"])
        total_b = repo_b + job_b
        pct = (total_b / grand_total * 100.0) if grand_total > 0 else 0.0
        rows.append((bt, repo_b, job_b, total_b, pct))

    rows.sort(key=lambda x: x[3], reverse=True)

    for bt, repo_b, job_b, total_b, pct in rows:
        ws.append(
            [
                bt,
                humanize.naturalsize(repo_b, binary=True),
                (humanize.naturalsize(job_b, binary=True) if job_b > 0 else ""),
                humanize.naturalsize(total_b, binary=True),
                round(pct, 2),
            ]
        )

    wb.save(out_path)
    log.info(f"Saved: {out_path} | rows={ws.max_row - 1} errors={errors}")
    log.info("Отчет завершен")


if __name__ == "__main__":
    main()
