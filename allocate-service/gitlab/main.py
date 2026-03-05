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
    # "15473",
    # "0",
}

BAN_BUSINESS_TYPES = [
    # "Retail",
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
    if not isinstance(ban_list, (list, tuple, set)):
        die("BAN_SERVICE_IDS должен быть list / tuple / set")
    return {str(x).strip() for x in ban_list if str(x).strip()}


def build_ban_business_types_set(ban_list):
    if not isinstance(ban_list, (list, tuple, set)):
        die("BAN_BUSINESS_TYPES должен быть list / tuple / set")
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
    map_by_name = {}

    for _, row in df.iterrows():
        num = normalize_number(row.iloc[1] if len(row) > 1 else "")
        sd_name_raw = row.iloc[3] if len(row) > 3 else ""
        owner_raw = row.iloc[7] if len(row) > 7 else ""

        sd_name = clean_spaces(sd_name_raw)
        owner = clean_spaces(owner_raw)

        payload = {"sd_name": sd_name, "owner": owner}

        if num:
            map_by_number[num] = payload
        if sd_name:
            map_by_name[normalize_name(sd_name)] = payload

    log.info("SD: загружено сервисов по номеру=%d, по имени=%d", len(map_by_number), len(map_by_name))
    return map_by_number, map_by_name


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
        s = (t or "").strip()
        m = SERVICE_ID_RE.match(s)
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


def resolve_sd_bk(service_id: str, map_by_number, map_by_name, bk_type_map):
    if not service_id:
        return "", "", ""

    sd = None
    norm_number = normalize_number(service_id)
    if norm_number and norm_number in map_by_number:
        sd = map_by_number[norm_number]
    else:
        sd = None

    sd_name = (sd or {}).get("sd_name") or ""
    owner = (sd or {}).get("owner") or ""
    business_type = bk_type_map.get(normalize_name_key(owner), "") if owner else ""

    return sd_name, owner, business_type


def main():
    validate_files()

    ban_set = build_ban_set(BAN_SERVICE_IDS)
    ban_business_types_set = build_ban_business_types_set(BAN_BUSINESS_TYPES)

    log.info("BAN_SERVICE_IDS=%s", sorted(ban_set) if ban_set else "[]")
    log.info("BAN_BUSINESS_TYPES=%s", sorted(ban_business_types_set) if ban_business_types_set else "[]")
    log.info("SKIP_EMPTY_BUSINESS_TYPE=%s", SKIP_EMPTY_BUSINESS_TYPE)

    bk_type_map = load_bk_business_type_map(BK_FILE)
    map_by_number, map_by_name = load_sd_mapping(SD_FILE)

    gl = connect()

    out_path = str(Path(OUT_XLSX).resolve())
    wb = Workbook()

    ws_sum = wb.active
    ws_sum.title = "ByServiceId"
    ws_sum.append(
        [
            "service_id",
            "sd_name",
            "owner",
            "business_type",
            "projects_count",
            "repo_size_b_sum",
            "repo_size_h_sum",
            "job_artifacts_b_sum",
            "job_artifacts_h_sum",
            "total_b_sum",
            "total_h_sum",
            "percent_of_all_total",
        ]
    )
    for c in ws_sum[1]:
        c.font = Font(bold=True)

    ws_unmapped = wb.create_sheet("Unmapped")
    ws_unmapped.append(
        [
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
        ]
    )
    for c in ws_unmapped[1]:
        c.font = Font(bold=True)

    agg = defaultdict(lambda: {"projects": 0, "repo": 0, "job": 0, "sd_name": "", "owner": "", "business_type": ""})

    total_repo_all = 0
    total_job_all = 0

    errors = 0
    processed = 0

    skipped_ban_sid = 0
    skipped_sd_miss = 0
    skipped_empty_bt = 0
    skipped_ban_bt = 0
    unmapped_missing = 0
    unmapped_conflict = 0

    log.info("Начинаем обход проектов...")

    for i, p in enumerate(gl.projects.list(all=True, iterator=True), start=1):
        if LIMIT and processed >= LIMIT:
            log.info("LIMIT=%d достигнут, останавливаемся", LIMIT)
            break

        proj_id = getattr(p, "id", None)

        try:
            full = gl.projects.get(proj_id, statistics=True)
            name = getattr(full, "path_with_namespace", "") or getattr(full, "name", "") or str(proj_id)
            web_url = getattr(full, "web_url", "") or ""

            topics = list(getattr(full, "topics", []) or [])
            service_id, sid_status = extract_service_id_info(topics)

            stats = getattr(full, "statistics", {}) or {}
            repo_bytes = int(stats.get("repository_size", 0) or 0)
            job_bytes = int(stats.get("job_artifacts_size", 0) or 0)
            total_bytes = repo_bytes + job_bytes

            total_repo_all += repo_bytes
            total_job_all += job_bytes

            sd_name = ""
            owner = ""
            business_type = ""

            if sid_status == "OK":
                sd_name, owner, business_type = resolve_sd_bk(service_id, map_by_number, map_by_name, bk_type_map)

                if service_id in ban_set:
                    skipped_ban_sid += 1
                    ws_unmapped.append(
                        [
                            "banned_service_id",
                            "service_id in BAN_SERVICE_IDS",
                            sid_status,
                            service_id,
                            sd_name,
                            owner,
                            business_type,
                            proj_id,
                            name,
                            web_url,
                            repo_bytes,
                            humanize.naturalsize(repo_bytes, binary=True),
                            job_bytes,
                            humanize.naturalsize(job_bytes, binary=True) if job_bytes else "",
                            total_bytes,
                            humanize.naturalsize(total_bytes, binary=True),
                        ]
                    )
                    processed += 1
                    continue

                if not sd_name and not owner:
                    skipped_sd_miss += 1
                    ws_unmapped.append(
                        [
                            "sd_mapping_miss",
                            "no match in SD by service_id",
                            sid_status,
                            service_id,
                            sd_name,
                            owner,
                            business_type,
                            proj_id,
                            name,
                            web_url,
                            repo_bytes,
                            humanize.naturalsize(repo_bytes, binary=True),
                            job_bytes,
                            humanize.naturalsize(job_bytes, binary=True) if job_bytes else "",
                            total_bytes,
                            humanize.naturalsize(total_bytes, binary=True),
                        ]
                    )
                    processed += 1
                    continue

                if SKIP_EMPTY_BUSINESS_TYPE and not clean_spaces(business_type):
                    skipped_empty_bt += 1
                    ws_unmapped.append(
                        [
                            "skip_empty_business_type",
                            "SKIP_EMPTY_BUSINESS_TYPE=True and business_type is empty",
                            sid_status,
                            service_id,
                            sd_name,
                            owner,
                            business_type,
                            proj_id,
                            name,
                            web_url,
                            repo_bytes,
                            humanize.naturalsize(repo_bytes, binary=True),
                            job_bytes,
                            humanize.naturalsize(job_bytes, binary=True) if job_bytes else "",
                            total_bytes,
                            humanize.naturalsize(total_bytes, binary=True),
                        ]
                    )
                    processed += 1
                    continue

                if ban_business_types_set and clean_spaces(business_type) in ban_business_types_set:
                    skipped_ban_bt += 1
                    ws_unmapped.append(
                        [
                            "banned_business_type",
                            "business_type in BAN_BUSINESS_TYPES",
                            sid_status,
                            service_id,
                            sd_name,
                            owner,
                            business_type,
                            proj_id,
                            name,
                            web_url,
                            repo_bytes,
                            humanize.naturalsize(repo_bytes, binary=True),
                            job_bytes,
                            humanize.naturalsize(job_bytes, binary=True) if job_bytes else "",
                            total_bytes,
                            humanize.naturalsize(total_bytes, binary=True),
                        ]
                    )
                    processed += 1
                    continue

                a = agg[service_id]
                a["projects"] += 1
                a["repo"] += repo_bytes
                a["job"] += job_bytes

                if not a["sd_name"] and sd_name:
                    a["sd_name"] = sd_name
                if not a["owner"] and owner:
                    a["owner"] = owner
                if not a["business_type"] and business_type:
                    a["business_type"] = business_type

            else:
                if sid_status == "MISSING":
                    unmapped_missing += 1
                    reason = "service_id_missing"
                    detail = "no service_id topic found"
                else:
                    unmapped_conflict += 1
                    reason = "service_id_conflict"
                    detail = "multiple different service_id topics"

                ws_unmapped.append(
                    [
                        reason,
                        detail,
                        sid_status,
                        service_id,
                        sd_name,
                        owner,
                        business_type,
                        proj_id,
                        name,
                        web_url,
                        repo_bytes,
                        humanize.naturalsize(repo_bytes, binary=True),
                        job_bytes,
                        humanize.naturalsize(job_bytes, binary=True) if job_bytes else "",
                        total_bytes,
                        humanize.naturalsize(total_bytes, binary=True),
                    ]
                )

            processed += 1

        except Exception as e:
            errors += 1
            log.warning("FAIL project_id=%s err=%s", proj_id, e)

        if LOG_EVERY and i % LOG_EVERY == 0:
            log.info("PROGRESS i=%d processed=%d errors=%d", i, processed, errors)

        if SLEEP_SEC:
            time.sleep(SLEEP_SEC)

    total_all = total_repo_all + total_job_all

    def _sort_key(x: str):
        return int(x) if str(x).isdigit() else str(x)

    for service_id in sorted(agg.keys(), key=_sort_key):
        repo_b = agg[service_id]["repo"]
        job_b = agg[service_id]["job"]
        total_b = repo_b + job_b

        ws_sum.append(
            [
                service_id,
                agg[service_id]["sd_name"],
                agg[service_id]["owner"],
                agg[service_id]["business_type"],
                agg[service_id]["projects"],
                repo_b,
                humanize.naturalsize(repo_b, binary=True),
                job_b,
                humanize.naturalsize(job_b, binary=True) if job_b else "",
                total_b,
                humanize.naturalsize(total_b, binary=True),
                round(pct(total_b, total_all), 4),
            ]
        )

    wb.save(out_path)

    log.info(
        "Saved: %s | projects=%d | services=%d | unmapped=%d | errors=%d | total=%s",
        out_path,
        processed,
        len(agg),
        ws_unmapped.max_row - 1,
        errors,
        humanize.naturalsize(total_all, binary=True),
    )
    log.info(
        "Unmapped breakdown: missing=%d conflict=%d ban_sid=%d sd_miss=%d empty_bt=%d ban_bt=%d",
        unmapped_missing,
        unmapped_conflict,
        skipped_ban_sid,
        skipped_sd_miss,
        skipped_empty_bt,
        skipped_ban_bt,
    )
    log.info("Готово")


if __name__ == "__main__":
    main()