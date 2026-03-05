import os
import re
import time
import logging
from pathlib import Path
from collections import defaultdict

import urllib3
import gitlab
import humanize
import pandas as pd
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

GITLAB_URL = (os.getenv("GITLAB_URL") or "").rstrip("/")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN") or ""

OUT_XLSX = os.getenv("OUT_XLSX", "gitlab_report.xlsx")

SD_FILE = os.getenv("SD_FILE", "sd.xlsx")
BK_FILE = os.getenv("BK_FILE", "bk_all_users.xlsx")

SSL_VERIFY = False
SLEEP_SEC = 0.02
LOG_EVERY = 100
LIMIT = 0

BAN_SERVICE_IDS = {
    "15473"
    "0"
}

BAN_BUSINESS_TYPES = [
]

SKIP_EMPTY_BUSINESS_TYPE = True

SERVICE_ID_RE = re.compile(r"^service[_-]?id\s*:\s*(\d+)\s*$", re.IGNORECASE)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("gitlab_sizes")


def die(msg: str, code: int = 2):
    log.error(msg)
    raise SystemExit(code)


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


def normalize_number(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".", 1)[0]
    return s


def normalize_name(x):
    if pd.isna(x):
        return None
    return str(x).strip().casefold()


def build_ban_set(ban_list):
    return {str(x).strip() for x in ban_list if str(x).strip()}


def build_ban_business_types_set(ban_list):
    return {clean_spaces(x) for x in ban_list if clean_spaces(x)}


def validate_files():
    if not SD_FILE or not os.path.isfile(SD_FILE):
        die(f"SD_FILE не найден: {SD_FILE}")
    if not BK_FILE or not os.path.isfile(BK_FILE):
        die(f"BK_FILE не найден: {BK_FILE}")


def load_bk_business_type_map(path: str):
    df = pd.read_excel(path, usecols="A:C,AS", dtype=str, engine="openpyxl").fillna("")
    df.columns = ["c1", "c2", "c3", "business_type"]

    def make_fio(r):
        fio = " ".join([clean_spaces(r["c2"]), clean_spaces(r["c1"]), clean_spaces(r["c3"])])
        return clean_spaces(fio)

    df["fio_key"] = df.apply(make_fio, axis=1).map(normalize_name_key)
    df["business_type"] = df["business_type"].astype(str).map(clean_spaces)

    last = df[df["fio_key"] != ""].drop_duplicates("fio_key", keep="last")
    mp = dict(zip(last["fio_key"], last["business_type"]))

    log.info("BK: загружено ФИО → тип бизнеса: %d", len(mp))
    return mp


def load_sd_mapping(path: str):
    df = pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    map_by_number = {}

    for _, row in df.iterrows():
        num = normalize_number(row.iloc[1] if len(row) > 1 else "")
        sd_name_raw = row.iloc[3] if len(row) > 3 else ""
        owner_raw = row.iloc[7] if len(row) > 7 else ""

        sd_name = clean_spaces(sd_name_raw)
        owner = clean_spaces(owner_raw)

        payload = {"sd_name": sd_name, "owner": owner}

        if num:
            map_by_number[num] = payload

    log.info("SD: загружено сервисов по номеру=%d", len(map_by_number))
    return map_by_number


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
    log.info("Подключение к GitLab успешно")
    return gl


def extract_service_id_info(topics):
    ids = []
    for t in topics or []:
        m = SERVICE_ID_RE.match((t or "").strip())
        if m:
            ids.append(m.group(1))

    if not ids:
        return "", "MISSING"

    uniq = sorted(set(ids))
    if len(uniq) == 1:
        return uniq[0], "OK"

    return "", "CONFLICT"


def pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return (part / total) * 100.0


def resolve_sd_bk(service_id: str, map_by_number, bk_type_map):
    if not service_id:
        return "", "", ""

    sd = map_by_number.get(service_id)
    sd_name = (sd or {}).get("sd_name") or ""
    owner = (sd or {}).get("owner") or ""
    business_type = bk_type_map.get(normalize_name_key(owner), "") if owner else ""

    return sd_name, owner, business_type


def main():
    validate_files()

    ban_set = build_ban_set(BAN_SERVICE_IDS)
    ban_business_types_set = build_ban_business_types_set(BAN_BUSINESS_TYPES)

    bk_type_map = load_bk_business_type_map(BK_FILE)
    map_by_number = load_sd_mapping(SD_FILE)

    gl = connect()

    out_path = str(Path(OUT_XLSX).resolve())
    wb = Workbook()

    ws_sum = wb.active
    ws_sum.title = "ByServiceId"

    ws_sum.append([
        "Тип бизнеса",
        "Наименование сервиса",
        "КОД",
        "Владелец",
        "кол-во проектов",
        "Объем репозиториев (bytes)",
        "Oбъем репозитория",
        "Объем джобов(bytes)",
        "Объем джобов",
        "Сумарнный объем",
        "% потребления",
    ])

    for c in ws_sum[1]:
        c.font = Font(bold=True)

    ws_unmapped = wb.create_sheet("Unmapped")

    ws_unmapped.append([
        "reason",
        "detail",
        "sid_status",
        "service_id",
        "sd_name",
        "owner",
        "business_type",
        "project_id",
        "project",
        "web_url",
        "repo_size_b",
        "repo_size_h",
        "job_artifacts_b",
        "job_artifacts_h",
        "total_b",
        "total_h",
    ])

    for c in ws_unmapped[1]:
        c.font = Font(bold=True)

    agg = defaultdict(lambda: {"projects": 0, "repo": 0, "job": 0, "sd_name": "", "owner": "", "business_type": ""})

    total_repo_all = 0
    total_job_all = 0

    log.info("Начинаем обход проектов...")

    for p in gl.projects.list(all=True, iterator=True):

        full = gl.projects.get(p.id, statistics=True)

        topics = list(getattr(full, "topics", []) or [])
        service_id, sid_status = extract_service_id_info(topics)

        stats = getattr(full, "statistics", {}) or {}
        repo_bytes = int(stats.get("repository_size", 0) or 0)
        job_bytes = int(stats.get("job_artifacts_size", 0) or 0)

        total_repo_all += repo_bytes
        total_job_all += job_bytes

        if sid_status != "OK":
            continue

        sd_name, owner, business_type = resolve_sd_bk(service_id, map_by_number, bk_type_map)

        a = agg[service_id]
        a["projects"] += 1
        a["repo"] += repo_bytes
        a["job"] += job_bytes
        a["sd_name"] = sd_name
        a["owner"] = owner
        a["business_type"] = business_type

        if SLEEP_SEC:
            time.sleep(SLEEP_SEC)

    total_all = total_repo_all + total_job_all

    for service_id in sorted(agg.keys()):

        repo_b = agg[service_id]["repo"]
        job_b = agg[service_id]["job"]
        total_b = repo_b + job_b

        ws_sum.append([
            agg[service_id]["business_type"],
            agg[service_id]["sd_name"],
            service_id,
            agg[service_id]["owner"],
            agg[service_id]["projects"],
            repo_b,
            humanize.naturalsize(repo_b, binary=True),
            job_b,
            humanize.naturalsize(job_b, binary=True) if job_b else "",
            humanize.naturalsize(total_b, binary=True),
            round(pct(total_b, total_all), 4),
        ])

    wb.save(out_path)

    log.info(
        "Saved: %s | services=%d | total=%s",
        out_path,
        len(agg),
        humanize.naturalsize(total_all, binary=True),
    )


if __name__ == "__main__":
    main()