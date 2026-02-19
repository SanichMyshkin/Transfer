import os
import logging

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


QUERY_PROJECTS = """
SELECT
    p."Id",
    p."Name",
    p."Description",
    COUNT(at."Id") AS "AutotestsCount",

    (SELECT COALESCE(SUM(jsonb_array_length(d2."Widgets")), 0)
     FROM "Dashboards" d2
     WHERE d2."ProjectId" = p."Id") AS "WidgetsCount",

    (SELECT COALESCE(SUM(tr."RunCount"), 0)
     FROM "TestRuns" tr
     WHERE tr."ProjectId" = p."Id"
       AND tr."IsAutomated" = TRUE
       AND (tr."IsDeleted" = FALSE OR tr."IsDeleted" IS NULL)) AS "AutoTestRunsCount",

    (SELECT COUNT(wi."Id")
     FROM "WorkItems" wi
     WHERE wi."ProjectId" = p."Id"
       AND wi."EntityTypeName" = 'TestCases'
       AND (wi."IsDeleted" = FALSE OR wi."IsDeleted" IS NULL)
       AND (wi."IsActual" = TRUE OR wi."IsActual" IS NULL)) AS "TestCasesCount",

    (SELECT COUNT(wi."Id")
     FROM "WorkItems" wi
     WHERE wi."ProjectId" = p."Id"
       AND wi."EntityTypeName" = 'CheckLists'
       AND (wi."IsDeleted" = FALSE OR wi."IsDeleted" IS NULL)
       AND (wi."IsActual" = TRUE OR wi."IsActual" IS NULL)) AS "CheckListsCount",

    (SELECT COUNT(wi."Id")
     FROM "WorkItems" wi
     WHERE wi."ProjectId" = p."Id"
       AND wi."EntityTypeName" = 'SharedSteps'
       AND (wi."IsDeleted" = FALSE OR wi."IsDeleted" IS NULL)
       AND (wi."IsActual" = TRUE OR wi."IsActual" IS NULL)) AS "SharedStepsCount",

    (SELECT COUNT(DISTINCT wi."Id")
     FROM "WorkItems" wi
     LEFT JOIN "WorkItemVersions" wiv ON wiv."WorkItemId" = wi."Id"
     LEFT JOIN "TestSuitesWorkItems" tswi
        ON tswi."WorkItemVersionId" = wiv."VersionId"
       AND (tswi."IsDeleted" = FALSE OR tswi."IsDeleted" IS NULL)
     WHERE wi."ProjectId" = p."Id"
       AND wi."IsDeleted" = FALSE
       AND wiv."VersionId" IS NOT NULL
       AND tswi."WorkItemVersionId" IS NOT NULL) AS "LibraryTestsCount",

    (SELECT COUNT(tp."Id")
     FROM "TestPlans" tp
     WHERE tp."ProjectId" = p."Id"
       AND (tp."IsDeleted" = FALSE OR tp."IsDeleted" IS NULL)) AS "TestPlansCount",

    (SELECT COUNT(wh."Id")
     FROM "WebHooks" wh
     WHERE wh."ProjectId" = p."Id"
       AND (wh."IsDeleted" = FALSE OR wh."IsDeleted" IS NULL)) AS "WebHooksCount",

    (SELECT COUNT(wl."Id")
     FROM "WebHookLogs" wl
     JOIN "WebHooks" wh ON wh."Id" = wl."WebHookId"
     WHERE wh."ProjectId" = p."Id"
       AND (wh."IsDeleted" = FALSE OR wh."IsDeleted" IS NULL)
       AND (wl."IsDeleted" = FALSE OR wl."IsDeleted" IS NULL))
       AS "WebHookLogsCount"

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


def write_sheet(wb: Workbook, sheet_name: str, rows):
    ws = wb.active
    ws.title = sheet_name

    if not rows:
        ws.cell(row=1, column=1, value="No data")
        return

    headers = list(rows[0].keys())

    # Заголовки
    for col, header in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = Font(bold=True)

    # Данные
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
    ) as conn_proj:
        projects = exec_query(conn_proj, QUERY_PROJECTS)

    logging.info("Projects loaded: %d", len(projects))

    wb = Workbook()
    write_sheet(wb, "Projects", projects)
    wb.save(OUT_XLSX)

    logging.info("Excel saved: %s", OUT_XLSX)


if __name__ == "__main__":
    main()
