import os
import re
import logging
from collections import defaultdict
from typing import Any, Optional, Tuple
from urllib.parse import urlparse

import humanize
from dotenv import load_dotenv
from opensearchpy import OpenSearch
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font

load_dotenv()


SD_FILE = os.getenv("SD_FILE")
BK_FILE = os.getenv("BK_FILE")

OPENSEARCH_URL = os.getenv("OPENSEARCH_URL")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))

USER = os.getenv("USER")
PASS = os.getenv("PASS")

BAN_SERVICE_IDS = {15473, 7788}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cpl")


TEAM_SERVICE_RE = re.compile(r"^index_([a-z0-9\-]+)-(\d+)_", re.IGNORECASE)
DIGITS_RE = re.compile(r"(\d+)")


def clean_spaces(s: str) -> str:
    return " ".join((s or "").replace(",", " ").split()).strip()


def humanize_bytes(n: int) -> str:
    return humanize.naturalsize(int(n or 0), binary=True)


def parse_host_and_ssl(raw: str, default_port: int) -> Tuple[str, int, bool]:
    if not raw:
        raise RuntimeError("OPENSEARCH_URL не задан")

    if raw.startswith("http://") or raw.startswith("https://"):
        u = urlparse(raw)
        return u.hostname, u.port or default_port, u.scheme == "https"

    return raw, default_port, True


def normalize_index_name(index_name: str) -> Optional[Tuple[str, int]]:
    m = TEAM_SERVICE_RE.match(index_name or "")
    if not m:
        return None
    return m.group(1).lower(), int(m.group(2))


def read_sd_map(path: str) -> dict[int, dict[str, str]]:
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]

    sd = {}
    for row in ws.iter_rows(values_only=True):
        m = DIGITS_RE.search(str(row[1] or ""))
        if not m:
            continue

        service_id = int(m.group(1))
        if service_id in sd:
            continue

        sd[service_id] = {
            "service_name": clean_spaces(row[3]),
            "owner": clean_spaces(row[7]),
        }

    wb.close()
    log.info(f"SD loaded: {len(sd)} services")
    return sd


def read_bk_map(path: str) -> dict[str, str]:
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.worksheets[0]

    out = {}
    for row in ws.iter_rows(values_only=True):
        if len(row) < 45:
            continue

        fio = clean_spaces(f"{row[1]} {row[0]} {row[2]}")
        business_type = clean_spaces(row[44])

        if not fio or not business_type:
            continue

        out.setdefault(fio, business_type)

    wb.close()
    log.info(f"BK loaded: {len(out)} owners")
    return out


def fetch_and_aggregate(client: OpenSearch) -> list[dict[str, Any]]:
    raw = client.cat.indices(format="json", bytes="b")

    acc = defaultdict(int)
    for idx in raw:
        parsed = normalize_index_name(idx.get("index"))
        if not parsed:
            continue

        team, service_id = parsed
        if service_id in BAN_SERVICE_IDS:
            continue

        acc[(team, service_id)] += int(idx.get("store.size", 0))

    rows = [
        {
            "team": team,
            "service_id": service_id,
            "total_bytes": total,
        }
        for (team, service_id), total in acc.items()
    ]

    log.info(f"OpenSearch aggregated: {len(rows)} services")
    return rows


def enrich(rows, sd, bk):
    for r in rows:
        meta = sd.get(r["service_id"], {})
        r["service_name"] = meta.get("service_name", "")
        r["owner"] = meta.get("owner", "")

        owner_norm = clean_spaces(r["owner"])
        r["business_type"] = bk.get(owner_norm, "")

    return rows


def finalize(rows):
    total_all = sum(r["total_bytes"] for r in rows)

    for r in rows:
        r["size_human"] = humanize_bytes(r["total_bytes"])
        r["pct"] = (r["total_bytes"] / total_all) if total_all else 0

    rows.sort(key=lambda x: x["total_bytes"], reverse=True)
    log.info(f"TOTAL: {humanize_bytes(total_all)}")
    return rows


def write_to_excel(path: str, rows):
    wb = Workbook()
    ws = wb.active
    ws.title = "CPL report"

    header = [
        "Тип бизнеса",
        "Наименование сервиса",
        "КОД",
        "Владелец",
        "Объем",
        "% потребления",
    ]
    ws.append(header)

    bold = Font(bold=True)
    for i in range(1, len(header) + 1):
        ws.cell(row=1, column=i).font = bold

    for r in rows:
        ws.append(
            [
                r["business_type"],
                r["service_name"],
                r["service_id"],
                r["owner"],
                r["size_human"],
                r["pct"],
            ]
        )

    for i in range(2, len(rows) + 2):
        ws.cell(row=i, column=6).number_format = "0.0000%"

    wb.save(path)
    log.info(f"Report written: {path}")


def main():
    host, port, use_ssl = parse_host_and_ssl(OPENSEARCH_URL, OPENSEARCH_PORT)

    client = OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_auth=(USER, PASS),
        use_ssl=use_ssl,
        verify_certs=False,
        ssl_show_warn=False,
    )

    rows = fetch_and_aggregate(client)
    sd = read_sd_map(SD_FILE)
    bk = read_bk_map(BK_FILE)

    rows = enrich(rows, sd, bk)
    rows = finalize(rows)

    write_to_excel("CPL_report.xlsx", rows)


if __name__ == "__main__":
    main()
