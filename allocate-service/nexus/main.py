# main.py
import os
import logging
import re
import urllib3
from collections import defaultdict

from dotenv import load_dotenv
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from humanfriendly import format_size

from nexus_sizes import get_repository_data, get_repository_sizes, get_kimb_top_folder_sizes
from confluence_names import confluence_table_as_dicts, repo_to_service_map


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SKIP_EMPTY_SERVICE = True

BAN_SERVICE_CODES = [
    15473,
]

BAN_BUSINESS_TYPES = []

SKIP_EMPTY_BUSINESS_TYPE = True

KIMB_REPO_NAME = "kimb-dependencies"


def clean_spaces(s):
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_name_key(s):
    return clean_spaces(s).lower()


def split_service_and_code(raw_service):
    s = clean_spaces(raw_service)
    if not s or s in {"-", "—"}:
        return "", ""

    parts = s.split("-")
    if len(parts) >= 2 and parts[-1].isdigit():
        return "-".join(parts[:-1]), parts[-1]

    m = re.search(r"(\d+)$", s)
    if m:
        code = m.group(1)
        name = s[: -len(code)].rstrip("-").strip()
        if name in {"-", "—"}:
            name = ""
        return name, code

    if s in {"-", "—"}:
        return "", ""

    return s, ""


def to_int_bytes(x):
    if x is None:
        return 0
    return int(x)


def build_ban_set(ban_list):
    return {str(x).strip() for x in ban_list if str(x).strip()}


BAN_SET = build_ban_set(BAN_SERVICE_CODES)
BAN_BUSINESS_SET = {clean_spaces(x) for x in BAN_BUSINESS_TYPES if clean_spaces(x)}


def read_sd_map(path):
    if not path or not os.path.exists(path):
        raise RuntimeError(f"SD_FILE не найден: {path}")

    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]

    sd = {}
    for row in ws.iter_rows(values_only=True):
        code_cell = row[1] if len(row) > 1 else ""
        name_cell = row[3] if len(row) > 3 else ""
        owner_cell = row[7] if len(row) > 7 else ""

        code_raw = str(code_cell or "")
        m = re.search(r"(\d+)", code_raw)
        if not m:
            continue
        code = m.group(1)

        sd_name = clean_spaces(str(name_cell or ""))
        owner = clean_spaces(str(owner_cell or ""))

        if code not in sd:
            sd[code] = {"sd_name": sd_name, "owner": owner}

    wb.close()
    return sd


def load_bk_business_type_map(path):
    if not path or not os.path.exists(path):
        return {}

    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]

    out = {}
    for row in ws.iter_rows(values_only=True):
        c1 = clean_spaces(str(row[0] or "")) if len(row) > 0 else ""
        c2 = clean_spaces(str(row[1] or "")) if len(row) > 1 else ""
        c3 = clean_spaces(str(row[2] or "")) if len(row) > 2 else ""
        business_type = clean_spaces(str(row[44] or "")) if len(row) > 44 else ""

        fio = clean_spaces(f"{c2} {c1} {c3}")
        fio_key = normalize_name_key(fio)
        if fio_key:
            out[fio_key] = business_type

    wb.close()
    return out


def write_excel(path, rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "report"

    header = ["Тип бизнеса", "Наименование сервиса", "КОД", "Владелец сервиса", "Объем", "% потребления"]
    ws.append(header)

    bold = Font(bold=True)
    for i in range(1, len(header) + 1):
        ws.cell(row=1, column=i).font = bold

    for r in rows:
        ws.append([
            r["business_type"],
            r["service_name"],
            r["code"],
            r["owner"],
            r["size_human"],
            r["percent"],
        ])

    widths = [len(h) for h in header]
    for r in rows:
        widths[0] = max(widths[0], len(str(r["business_type"])))
        widths[1] = max(widths[1], len(str(r["service_name"])))
        widths[2] = max(widths[2], len(str(r["code"])))
        widths[3] = max(widths[3], len(str(r["owner"])))
        widths[4] = max(widths[4], len(str(r["size_human"])))
        widths[5] = max(widths[5], len(str(r["percent"])))

    for idx, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(idx)].width = min(max(w + 2, 12), 60)

    wb.save(path)


def _merge_kimb_into_totals(log, totals, service_to_repos, kimb_details, not_counted, banned_repos):
    folder_sizes = get_kimb_top_folder_sizes(KIMB_REPO_NAME)

    groups = {}
    for folder, size_bytes in folder_sizes.items():
        base, code = split_service_and_code(folder)
        base = clean_spaces(base)
        display = base or folder
        name_key = normalize_name_key(base) if base else normalize_name_key(folder)
        if not name_key:
            continue

        g = groups.get(name_key)
        if not g:
            g = {
                "size_bytes": 0,
                "codes": set(),
                "code_bytes": {},
                "display_name": display,
                "folders": [],
            }
            groups[name_key] = g

        b = to_int_bytes(size_bytes)
        g["size_bytes"] += b
        g["folders"].append((folder, b))

        if display and len(display) > len(g["display_name"] or ""):
            g["display_name"] = display

        if code:
            g["codes"].add(code)
            g["code_bytes"][code] = g["code_bytes"].get(code, 0) + b

    log.info("KIMB scan: folders=%d groups=%d", len(folder_sizes), len(groups))

    kimb_groups_without_code = 0
    kimb_conflicts = 0
    kimb_mapped = 0
    kimb_banned = 0

    for _, g in groups.items():
        codes = sorted(g["codes"])
        chosen_code = ""

        if len(codes) == 0:
            kimb_groups_without_code += 1
            not_counted.append(
                {
                    "repo": KIMB_REPO_NAME,
                    "unit": g["display_name"],
                    "reason": "kimb_group_without_code",
                    "size_bytes": g["size_bytes"],
                }
            )
            log.warning(
                "KIMB NOT_COUNTED group=%s size=%s reason=without_code folders=%s",
                g["display_name"],
                format_size(g["size_bytes"], binary=True),
                ", ".join([f"{f}:{format_size(b, binary=True)}" for f, b in g["folders"][:20]]) + (" ..." if len(g["folders"]) > 20 else ""),
            )
            continue

        if len(codes) == 1:
            chosen_code = codes[0]
        else:
            kimb_conflicts += 1
            chosen_code = max(g["code_bytes"].items(), key=lambda kv: kv[1])[0]
            log.warning(
                "KIMB CONFLICT group=%s codes=%s chosen=%s",
                g["display_name"],
                ",".join(codes),
                chosen_code,
            )

        if chosen_code in BAN_SET:
            kimb_banned += 1
            banned_repos.append(
                {
                    "repo": KIMB_REPO_NAME,
                    "unit": g["display_name"],
                    "code": chosen_code,
                    "reason": "banned_service_code",
                    "size_bytes": g["size_bytes"],
                }
            )
            log.info(
                "KIMB BANNED group=%s code=%s size=%s",
                g["display_name"],
                chosen_code,
                format_size(g["size_bytes"], binary=True),
            )
            continue

        if chosen_code not in totals:
            totals[chosen_code] = {"size_bytes": 0, "base_name": g["display_name"]}

        totals[chosen_code]["size_bytes"] += g["size_bytes"]
        if g["display_name"] and len(g["display_name"]) > len(totals[chosen_code]["base_name"] or ""):
            totals[chosen_code]["base_name"] = g["display_name"]

        service_to_repos[chosen_code].add(f"{KIMB_REPO_NAME}:{g['display_name']}")
        kimb_details.append(
            {
                "service_key": g["display_name"],
                "code": chosen_code,
                "size_bytes": g["size_bytes"],
                "folders": g["folders"],
            }
        )

        kimb_mapped += 1
        log.info(
            "KIMB MAP service=%s code=%s size=%s folders=%s",
            g["display_name"],
            chosen_code,
            format_size(g["size_bytes"], binary=True),
            ", ".join([f"{f}:{format_size(b, binary=True)}" for f, b in g["folders"][:20]]) + (" ..." if len(g["folders"]) > 20 else ""),
        )

    log.info("KIMB summary: mapped=%d without_code=%d conflicts=%d banned=%d", kimb_mapped, kimb_groups_without_code, kimb_conflicts, kimb_banned)


def main():
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("nexus_report")

    conf_url = os.getenv("CONF_URL", "").strip()
    conf_page_id = os.getenv("CONF_PAGE_ID", "").strip()
    conf_user = os.getenv("CONF_USER", "").strip()
    conf_pass = os.getenv("CONF_PASS", "").strip()

    sd_file = os.getenv("SD_FILE", "sd.xlsx").strip()
    bk_file = os.getenv("BK_FILE", "bk_all_users.xlsx").strip()
    out_file = os.getenv("OUT_FILE", "nexus_service_consumption.xlsx").strip()

    if not conf_url or not conf_page_id or not conf_user or not conf_pass:
        raise RuntimeError("Нужны CONF_URL, CONF_PAGE_ID, CONF_USER, CONF_PASS")

    log.info("Читаю таблицу из Confluence")
    conf_rows = confluence_table_as_dicts(conf_url, conf_page_id, conf_user, conf_pass)
    repo_service = repo_to_service_map(conf_rows)

    log.info("Читаю SD и BK")
    sd_map = read_sd_map(sd_file)
    bk_map = load_bk_business_type_map(bk_file)

    log.info("Читаю репозитории из БД")
    repo_data = get_repository_data()

    log.info("Считаю размеры репозиториев из БД")
    repo_sizes = get_repository_sizes()

    totals = {}
    service_to_repos = defaultdict(set)

    not_counted = []
    banned_repos = []

    hosted_total = 0
    non_hosted_total = 0

    skipped_no_service = 0
    skipped_no_code = 0
    skipped_ban_service_code = 0

    kimb_details = []

    log.info("=== DETAILED MAPPING (repo -> service) ===")

    for r in repo_data:
        repo_type = (r.get("repository_type") or "").strip().lower()
        repo_name = r.get("repository_name") or ""

        if repo_type != "hosted":
            non_hosted_total += 1
            not_counted.append(
                {"repo": repo_name, "unit": repo_name, "reason": f"non_hosted:{repo_type}", "size_bytes": to_int_bytes(repo_sizes.get(repo_name))}
            )
            continue

        hosted_total += 1

        if repo_name == KIMB_REPO_NAME:
            continue

        raw_service = repo_service.get(repo_name, "")
        base_name, code = split_service_and_code(raw_service)

        if SKIP_EMPTY_SERVICE and not base_name:
            skipped_no_service += 1
            not_counted.append(
                {"repo": repo_name, "unit": repo_name, "reason": "no_service_mapping", "size_bytes": to_int_bytes(repo_sizes.get(repo_name))}
            )
            log.warning(
                "NOT_COUNTED repo=%s reason=no_service_mapping size=%s",
                repo_name,
                format_size(to_int_bytes(repo_sizes.get(repo_name)), binary=True),
            )
            continue

        if not code:
            skipped_no_code += 1
            not_counted.append(
                {"repo": repo_name, "unit": repo_name, "reason": "no_service_code", "size_bytes": to_int_bytes(repo_sizes.get(repo_name))}
            )
            log.warning(
                "NOT_COUNTED repo=%s service=%s reason=no_code size=%s",
                repo_name,
                base_name,
                format_size(to_int_bytes(repo_sizes.get(repo_name)), binary=True),
            )
            continue

        if code in BAN_SET:
            skipped_ban_service_code += 1
            size_bytes = to_int_bytes(repo_sizes.get(repo_name))
            banned_repos.append(
                {"repo": repo_name, "unit": repo_name, "code": code, "reason": "banned_service_code", "size_bytes": size_bytes}
            )
            log.info(
                "BANNED repo=%s service=%s code=%s size=%s",
                repo_name,
                base_name,
                code,
                format_size(size_bytes, binary=True),
            )
            continue

        size_bytes = to_int_bytes(repo_sizes.get(repo_name))

        if code not in totals:
            totals[code] = {"size_bytes": 0, "base_name": base_name}
        totals[code]["size_bytes"] += size_bytes
        if base_name and len(base_name) > len(totals[code]["base_name"] or ""):
            totals[code]["base_name"] = base_name

        service_to_repos[code].add(repo_name)

        log.info(
            "MAP repo=%s -> service=%s code=%s size=%s",
            repo_name,
            base_name,
            code,
            format_size(size_bytes, binary=True),
        )

    log.info("Считаю kimb-dependencies по верхним папкам")
    _merge_kimb_into_totals(log, totals, service_to_repos, kimb_details, not_counted, banned_repos)

    candidates = []
    skipped_empty_business_type = 0
    skipped_ban_business_type = 0

    skipped_by_business = []
    skipped_by_business_ban = []

    for code, v in totals.items():
        size_bytes = v["size_bytes"]
        base_name = v["base_name"]

        sd = sd_map.get(code, {})
        service_name = sd.get("sd_name") or base_name
        owner = sd.get("owner") or ""

        business_type = ""
        if owner:
            business_type = bk_map.get(normalize_name_key(owner), "")
        business_type = clean_spaces(business_type)

        if SKIP_EMPTY_BUSINESS_TYPE and not business_type:
            skipped_empty_business_type += 1
            skipped_by_business.append(
                {"code": code, "service_name": service_name, "owner": owner, "size_bytes": size_bytes, "repos": sorted(service_to_repos.get(code, []))}
            )
            continue

        if BAN_BUSINESS_SET and business_type in BAN_BUSINESS_SET:
            skipped_ban_business_type += 1
            skipped_by_business_ban.append(
                {"code": code, "service_name": service_name, "business_type": business_type, "owner": owner, "size_bytes": size_bytes, "repos": sorted(service_to_repos.get(code, []))}
            )
            continue

        candidates.append(
            {
                "business_type": business_type,
                "service_name": service_name,
                "code": code,
                "owner": owner,
                "size_bytes": size_bytes,
            }
        )

    eligible_total = sum(x["size_bytes"] for x in candidates)

    rows = []
    for x in candidates:
        size_bytes = x["size_bytes"]
        percent = 0.0
        if eligible_total > 0:
            percent = (size_bytes / eligible_total) * 100.0

        rows.append(
            {
                "business_type": x["business_type"],
                "service_name": x["service_name"],
                "code": x["code"],
                "owner": x["owner"],
                "size_bytes": size_bytes,
                "size_human": format_size(size_bytes, binary=True),
                "percent": round(percent, 4),
            }
        )

    rows.sort(key=lambda x: x["size_bytes"], reverse=True)

    log.info("=== SUMMARY (service -> repos) ===")
    for r in rows:
        code = r["code"]
        repos = sorted(service_to_repos.get(code, []))
        log.info(
            "SERVICE code=%s business=%s name=%s owner=%s total=%s repos=%s",
            code,
            r["business_type"],
            r["service_name"],
            r["owner"],
            r["size_human"],
            ", ".join(repos) if repos else "-",
        )

    log.info("=== KIMB SUMMARY (group -> code -> size) ===")
    for x in sorted(kimb_details, key=lambda z: z["size_bytes"], reverse=True):
        log.info(
            "KIMB_SERVICE name=%s code=%s size=%s folders=%s",
            x["service_key"],
            x["code"],
            format_size(x["size_bytes"], binary=True),
            ", ".join([f"{f}:{format_size(b, binary=True)}" for f, b in x["folders"][:20]]) + (" ..." if len(x["folders"]) > 20 else ""),
        )

    log.info("=== NOT COUNTED REPOS/UNITS ===")
    for x in sorted(not_counted, key=lambda z: z.get("size_bytes", 0), reverse=True):
        log.info(
            "NOT_COUNTED repo=%s unit=%s reason=%s size=%s",
            x.get("repo"),
            x.get("unit"),
            x.get("reason"),
            format_size(to_int_bytes(x.get("size_bytes")), binary=True),
        )

    log.info("=== BANNED REPOS/UNITS ===")
    for x in sorted(banned_repos, key=lambda z: z.get("size_bytes", 0), reverse=True):
        log.info(
            "BANNED repo=%s unit=%s code=%s reason=%s size=%s",
            x.get("repo"),
            x.get("unit"),
            x.get("code"),
            x.get("reason"),
            format_size(to_int_bytes(x.get("size_bytes")), binary=True),
        )

    log.info("=== SKIPPED BY BUSINESS TYPE (empty) ===")
    for x in sorted(skipped_by_business, key=lambda z: z.get("size_bytes", 0), reverse=True):
        log.info(
            "SKIP_BUSINESS_EMPTY code=%s service=%s owner=%s size=%s repos=%s",
            x["code"],
            x["service_name"],
            x["owner"],
            format_size(x["size_bytes"], binary=True),
            ", ".join(x["repos"]) if x["repos"] else "-",
        )

    log.info("=== SKIPPED BY BUSINESS TYPE (ban) ===")
    for x in sorted(skipped_by_business_ban, key=lambda z: z.get("size_bytes", 0), reverse=True):
        log.info(
            "SKIP_BUSINESS_BAN code=%s business=%s service=%s owner=%s size=%s repos=%s",
            x["code"],
            x["business_type"],
            x["service_name"],
            x["owner"],
            format_size(x["size_bytes"], binary=True),
            ", ".join(x["repos"]) if x["repos"] else "-",
        )

    log.info("hosted repos: %d", hosted_total)
    log.info("non-hosted repos: %d", non_hosted_total)
    log.info("skipped without service: %d", skipped_no_service)
    log.info("skipped without code: %d", skipped_no_code)
    log.info("skipped by service code ban: %d", skipped_ban_service_code)
    log.info("skipped empty business type: %d", skipped_empty_business_type)
    log.info("skipped by business type ban: %d", skipped_ban_business_type)
    log.info("services in report: %d", len(rows))
    log.info("eligible_total: %s", format_size(eligible_total, binary=True))
    log.info("write excel: %s", out_file)

    write_excel(out_file, rows)

    log.info("done")


if __name__ == "__main__":
    main()