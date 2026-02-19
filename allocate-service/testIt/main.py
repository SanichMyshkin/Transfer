import os
import logging
import re

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s"
)

load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DB = os.getenv("PG_DB")

OUT_XLSX = os.getenv("OUT_XLSX", "testIt_projects.xlsx")

PFP_RE = re.compile(r"пфп\s*[-–—:]?\s*(\d+)", re.IGNORECASE)


QUERY_PROJECTS = """
SELECT
    p."Id",
    p."Name",
    p."Description",
    COUNT(DISTINCT at."Id") AS "AutotestsCount",
    (
        SELECT COUNT(wi."Id")
        FROM "WorkItems" wi
        WHERE wi."ProjectId" = p."Id"
          AND wi."EntityTypeName" = 'TestCases'
          AND (wi."IsDeleted" = FALSE OR wi."IsDeleted" IS NULL)
          AND (wi."IsActual" = TRUE OR wi."IsActual" IS NULL)
    ) AS "TestCasesCount"
FROM "Projects" p
LEFT JOIN "AutoTests" at ON at."ProjectId" = p."Id"
GROUP BY p."Id", p."Name", p."Description"
ORDER BY p."Id";
"""


def die(msg: str, code: int = 2):
    logging.error(msg)
    raise SystemExit(code)


def exec_query(conn, query: str):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        return cur.fetchall()


def extract_pfp(description: str):
    if not description:
        return None
    m = PFP_RE.search(description)
    return m.group(1) if m else None


def write_sheet(wb: Workbook, sheet_name: str, rows):
    ws = wb.active
    ws.title = sheet_name

    if not rows:
        ws.cell(row=1, column=1, value="No data")
        return

    headers = list(rows[0].keys())
    header_font = Font(bold=True)

    for col, header in enumerate(headers, start=1):
        c = ws.cell(row=1, column=col, value=header)
        c.font = header_font

    for row_idx, row_data in enumerate(rows, start=2):
        for col_idx, header in enumerate(headers, start=1):
            ws.cell(row=row_idx, column=col_idx, value=row_data.get(header))


def main():
    for k in ("PG_HOST", "PG_PORT", "PG_USER", "PG_PASSWORD", "PG_DB"):
        if not globals().get(k):
            die(f"Env var {k} is not set")

    logging.info("Connecting to PROJECT DB...")
    with psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    ) as conn:
        projects = exec_query(conn, QUERY_PROJECTS)

    logging.info("Projects loaded: %d", len(projects))
    for p in projects:
        p["PFP"] = extract_pfp(p.get("Description"))

    ordered = []
    for p in projects:
        ordered.append(
            {
                "Id": p.get("Id"),
                "Name": p.get("Name"),
                "Description": p.get("Description"),
                "PFP": p.get("PFP"),
                "TestCasesCount": p.get("TestCasesCount"),
                "AutotestsCount": p.get("AutotestsCount"),
            }
        )

    wb = Workbook()
    write_sheet(wb, "Projects", ordered)
    wb.save(OUT_XLSX)

    logging.info("Excel saved: %s", OUT_XLSX)


if __name__ == "__main__":
    main()
