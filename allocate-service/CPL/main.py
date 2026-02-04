import os
import re
import logging
import humanize
from collections import defaultdict
from typing import Any, Optional, Tuple
from urllib.parse import urlparse

from dotenv import load_dotenv
from opensearchpy import OpenSearch
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cpl")

SD_FILE = os.getenv("SD_FILE")
BK_FILE = os.getenv("BK_FILE")

OPENSEARCH_URL = os.getenv("OPENSEARCH_URL") or os.getenv("OPENSERACH_URL")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", "9200"))

USER = os.getenv("USER")
PASS = os.getenv("PASS")

BAN_SERVICE_IDS_RAW = os.getenv("BAN_SERVICE_IDS", "")
BAN_SERVICE_IDS = set(re.findall(r"\d+", BAN_SERVICE_IDS_RAW))

TEAM_SERVICE_RE = re.compile(r"^index_([a-z0-9\-]+)-(\d+)_", re.IGNORECASE)
DIGITS_RE = re.compile(r"(\d+)")


def clean_spaces(s: str) -> str:
    s = (s or "").replace(",", " ")
    return " ".join(s.split()).strip()


def humanize_bytes(n: int) -> str:
    return humanize.naturalsize(int(n or 0), binary=True)


def parse_host_and_ssl(raw: str, default_port: int) -> Tuple[str, int, bool]:
    if not raw:
        raise RuntimeError("OPENSEARCH_URL/OPENSERACH_URL не задан")

    raw = raw.strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        u = urlparse(raw)
        host = u.hostname
        port = u.port or default_port
        use_ssl = u.scheme == "https"
        if not host:
            raise RuntimeError(f"Не удалось распарсить host из URL: {raw}")
        return host, port, use_ssl

    return raw, default_port, True


def normalize_index_name(index_name: str) -> Optional[Tuple[str, str]]:
    m = TEAM_SERVICE_RE.match(index_name or "")
    if not m:
        return None
    team = m.group(1).lower().strip()
    service_id = m.group(2)
    return team, service_id


def read_sd_map(path: str) -> dict[str, dict[str, str]]:
    if not path or not os.path.exists(path):
        raise RuntimeError(f"SD_FILE не найден: {path}")

    wb = load_workbook(filename=path, read_only=True, data_only=True)
    ws = wb.worksheets[0]

    sd: dict[str, dict[str, str]] = {}
    for row in ws.iter_rows(values_only=True):
        code_cell = row[1] if len(row) > 1 else ""
        name_cell = row[3] if len(row) > 3 else ""
        owner_cell = row[7] if len(row) > 7 else ""

        m = DIGITS_RE.search(str(code_cell or ""))
        if not m:
            continue
        code = m.group(1)

        if code in sd:
            continue

        sd_name = clean_spaces(str(name_cell or ""))
        owner = clean_spaces(str(owner_cell or ""))

        sd[code] = {"sd_name": sd_name, "owner": owner}

    wb.close()
    log.info(f"SD: services={len(sd)}")
    return sd


def read_bk_map(path: str) -> dict[str, str]:
    if not path or not os.path.exists(path):
        raise RuntimeError(f"BK_FILE не найден: {path}")

    wb = load_workbook(filename=path, read_only=True, data_only=True)
    ws = wb.worksheets[0]

    out: dict[str, str] = {}
    for row in ws.iter_rows(values_only=True):
        if len(row) < 45:
            continue

        c1 = clean_spaces(str(row[0] or ""))
        c2 = clean_spaces(str(row[1] or ""))
        c3 = clean_spaces(str(row[2] or ""))

        business_type = clean_spaces(str(row[44] or ""))

        fio = clean_spaces(f"{c2} {c1} {c3}")
        if not fio or fio.lower() == "фио":
            continue
        if not business_type or business_type.lower() == "тип бизнеса":
            continue

        out.setdefault(fio, business_type)

    wb.close()
    log.info(f"BK: owners={len(out)}")
    return out


def fetch_and_aggregate(client: OpenSearch) -> list[dict[str, Any]]:
    raw = client.cat.indices(format="json", bytes="b")

    acc = defaultdict(int)
    matched = 0

    for idx in raw:
        index_name = idx.get("index", "")
        parsed = normalize_index_name(index_name)
        if not parsed:
            continue

        team, service_id = parsed
        if service_id in BAN_SERVICE_IDS:
            continue

        size_b = int(idx.get("store.size", "0"))
        acc[(team, service_id)] += size_b
        matched += 1

    rows = [
        {"team": team, "service_id": service_id, "total_size_bytes": total_b}
        for (team, service_id), total_b in acc.items()
    ]

    log.info(
        f"OS: indices={len(raw)} matched={matched} services={len(rows)} banned={len(BAN_SERVICE_IDS)}"
    )
    return rows


def enrich(
    rows: list[dict[str, Any]], sd: dict[str, dict[str, str]], bk: dict[str, str]
) -> list[dict[str, Any]]:
    sd_hit = 0
    bk_hit = 0

    for r in rows:
        sid = str(r.get("service_id") or "")
        meta = sd.get(sid)

        if meta:
            sd_hit += 1
            r["service_name"] = meta.get("sd_name", "")
            r["owner"] = meta.get("owner", "")
        else:
            r["service_name"] = ""
            r["owner"] = ""

        owner_norm = clean_spaces(r.get("owner", ""))
        bt = bk.get(owner_norm, "")
        if bt:
            bk_hit += 1
        r["business_type"] = bt

    log.info(f"ENRICH: sd_hit={sd_hit}/{len(rows)} bk_hit={bk_hit}/{len(rows)}")
    return rows


def finalize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total_all = sum(int(r.get("total_size_bytes", 0)) for r in rows) or 0

    for r in rows:
        b = int(r.get("total_size_bytes", 0))
        r["size_human"] = humanize_bytes(b)
        r["pct_of_total"] = (b / total_all) if total_all else 0.0

    rows.sort(key=lambda x: int(x.get("total_size_bytes", 0)), reverse=True)
    log.info(
        f"TOTAL: bytes={total_all} ({humanize_bytes(total_all)}) services={len(rows)}"
    )
    return rows


def write_to_excel(path: str, rows: list[dict[str, Any]]):
    wb = Workbook()
    ws = wb.active
    ws.title = "Отчет CPL"

    header = ["Тип бизнеса", "Service name", "КОД", "Владелец", "Объем", "% от объема"]
    ws.append(header)

    bold = Font(bold=True)
    for i in range(1, len(header) + 1):
        ws.cell(row=1, column=i).font = bold

    for r in rows:
        ws.append(
            [
                r.get("business_type", ""),
                r.get("service_name", ""),
                r.get("service_id", ""),
                r.get("owner", ""),
                r.get("size_human", ""),
                r.get("pct_of_total", 0.0),
            ]
        )

    for row_idx in range(2, 2 + len(rows)):
        ws.cell(row=row_idx, column=6).number_format = "0.0000%"

    wb.save(path)
    log.info(f"WROTE: {path} rows={len(rows)}")


def main():
    host, port, use_ssl = parse_host_and_ssl(OPENSEARCH_URL, OPENSEARCH_PORT)
    log.info(f"OS endpoint: {host}:{port} ssl={use_ssl}")

    if not USER or not PASS:
        raise RuntimeError("USER/PASS не заданы")

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
