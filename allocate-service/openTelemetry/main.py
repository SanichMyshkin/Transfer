#!/usr/bin/env python3
import os
import sys
import logging
import re

import clickhouse_connect
import humanize
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font

load_dotenv()

HOST = os.getenv("CH_HOST")
PORT = int(os.getenv("CH_PORT", 8123))
USER = os.getenv("CH_USER", "default")
PASSWORD = os.getenv("CH_PASSWORD", "")
DATABASE = os.getenv("CH_DATABASE")
SECURE = False
VERIFY = False
OUTPUT_FILE = os.getenv("CH_OUT", "openTelemetry_report.xlsx")

LOG = logging.getLogger("ch_table_sizes")

SQL = """
SELECT
    t.name AS table,
    coalesce(sum(p.bytes_on_disk), 0) AS bytes_on_disk
FROM system.tables AS t
LEFT JOIN system.parts AS p
    ON p.database = t.database
   AND p.table = t.name
   AND p.active
WHERE t.database = {db:String}
  AND t.engine NOT IN ('View', 'MaterializedView', 'LiveView')
  AND t.engine != 'Distributed'
GROUP BY table
ORDER BY bytes_on_disk DESC
"""


TABLE_PREFIX_OVERRIDES = {
    # "otel_traces": ("SOMESVC", 12345),
}

NAME_RE = re.compile(r"^otel_([a-z0-9]+)_(\d+)_traces(?:_trace_id_ts)?$", re.IGNORECASE)


def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def map_table(table_name: str):
    for base, (svc_name, svc_id) in TABLE_PREFIX_OVERRIDES.items():
        if table_name == base or table_name == f"{base}_trace_id_ts":
            return svc_name, int(svc_id), "override"

    m = NAME_RE.match(table_name)
    if not m:
        return None, None, "unmatched"

    return m.group(1).upper(), int(m.group(2)), "parsed"


def main() -> int:
    setup_logging()

    if not HOST or not DATABASE:
        LOG.error("CH_HOST and CH_DATABASE must be set")
        return 1

    LOG.info("Connecting to %s:%s secure=%s", HOST, PORT, SECURE)

    try:
        client = clickhouse_connect.get_client(
            host=HOST,
            port=PORT,
            username=USER,
            password=PASSWORD,
            secure=SECURE,
            verify=VERIFY,
        )
    except Exception:
        LOG.exception("Connection failed")
        return 2

    try:
        LOG.info("Executing query for database=%s", DATABASE)
        res = client.query(SQL, parameters={"db": DATABASE})
        table_rows = res.result_rows
        LOG.info("Query returned %d tables (including empty)", len(table_rows))
    except Exception:
        LOG.exception("Query failed")
        return 3

    agg = {}
    unmatched = 0

    for table, bytes_on_disk in table_rows:
        b = int(bytes_on_disk or 0)

        service_name, service_id, src = map_table(table)
        if src == "unmatched" or service_id is None:
            unmatched += 1
            LOG.warning("Unmatched table (not accounted): %s (bytes=%d)", table, b)
            continue

        if service_id not in agg:
            agg[service_id] = {"service_name": service_name or "", "bytes": 0}
        else:
            if not agg[service_id]["service_name"] and service_name:
                agg[service_id]["service_name"] = service_name

        agg[service_id]["bytes"] += b

    total_accounted = sum(v["bytes"] for v in agg.values())

    wb = Workbook()
    ws = wb.active
    ws.title = "ByService"

    headers = [
        "service_name",
        "service_id",
        "bytes_on_disk",
        "size_on_disk_h",
        "percent_of_total",
    ]
    ws.append(headers)

    bold = Font(bold=True)
    for i in range(1, len(headers) + 1):
        ws.cell(row=1, column=i).font = bold

    items = sorted(
        ((sid, v["service_name"], v["bytes"]) for sid, v in agg.items()),
        key=lambda x: x[2],
        reverse=True,
    )

    for service_id, service_name, bytes_sum in items:
        pct = (bytes_sum / total_accounted * 100.0) if total_accounted > 0 else 0.0
        ws.append([
            service_name,
            service_id,
            bytes_sum,
            humanize.naturalsize(bytes_sum, binary=True),
            round(pct, 6),
        ])

    wb.save(OUTPUT_FILE)

    LOG.info(
        "Report saved: %s | services=%d | tables_total=%d | unmatched_tables=%d | total_accounted=%s",
        OUTPUT_FILE,
        len(agg),
        len(table_rows),
        unmatched,
        humanize.naturalsize(total_accounted, binary=True),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())