import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import xlsxwriter
import sqlite3
import re

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s"
)

load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DB = os.getenv("PG_DB")
PG_DB2 = os.getenv("PG_DB2")

BK_SQLITE_PATH = os.getenv('BK_SQLITE_PATH')

def exec_query(conn, query):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        return cur.fetchall()


QUERY_USERS = """
SELECT 
    u."Id" AS user_id,
    u."UserName",
    u."Email",
    u."FirstName",
    u."MiddleName",
    u."LastName",
    u."CreatedBy",
    u."UserType", 
    u."DistinguishedName",
    ARRAY_AGG(DISTINCT g."Name") AS group_names,
    ARRAY_AGG(DISTINCT r."Name") AS role_names
FROM "AspNetUsers" u
LEFT JOIN "UserGroups" ug ON u."Id" = ug."UserId"
LEFT JOIN "Groups" g ON ug."GroupId" = g."Id"
INNER JOIN "UserRoles" ur ON u."Id" = ur."UserId"
INNER JOIN "AspNetRoles" r ON ur."RoleId" = r."Id"
GROUP BY u."Id", u."UserName", u."Email", u."FirstName",
         u."MiddleName", u."LastName",
         u."CreatedBy", u."UserType", u."DistinguishedName"
ORDER BY u."UserName";
"""


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


def write_sheet(workbook, sheet_name, rows):
    sheet = workbook.add_worksheet(sheet_name)
    if not rows:
        return
    headers = list(rows[0].keys())
    for col, h in enumerate(headers):
        sheet.write(0, col, h)
    for i, r in enumerate(rows, start=1):
        for j, h in enumerate(headers):
            sheet.write(i, j, str(r[h]) if r[h] is not None else "")


def write_summary(workbook, users, projects):
    sheet = workbook.add_worksheet("Summary")
    rows = [
        ("Users count", len(users)),
        ("Projects count", len(projects)),
        ("Total test-cases", sum(p["TestCasesCount"] for p in projects)),
        ("Total autotests", sum(p["AutotestsCount"] for p in projects)),
        ("Total test-plans", sum(p["TestPlansCount"] for p in projects)),
        ("Total library tests", sum(p["LibraryTestsCount"] for p in projects)),
        ("Total webhooks created", sum(p["WebHooksCount"] for p in projects)),
        ("Total webhook runs", sum(p["WebHookLogsCount"] for p in projects)),
        ("Active users (in BK)", len([u for u in users if u.get("Status") == "Active"])),
        ("Fired users", len([u for u in users if u.get("Status") == "Fired"])),
        ("Tech accounts", len([u for u in users if u.get("Status") == "Tech"])),
    ]
    sheet.write(0, 0, "Metric")
    sheet.write(0, 1, "Value")
    for i, (m, v) in enumerate(rows, start=1):
        sheet.write(i, 0, m)
        sheet.write(i, 1, v)


def is_cyrillic(s: str) -> bool:
    return bool(re.search(r"[а-яА-Я]", s))


def is_full_name(display: str) -> bool:
    if not display:
        return False
    if not is_cyrillic(display):
        return False
    parts = display.strip().split()
    return len(parts) >= 2


def classify_tech_account(display: str) -> bool:
    return not is_full_name(display)


def main():
    logging.info("Connecting to AUTH DB...")
    with psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    ) as conn_users:
        users = exec_query(conn_users, QUERY_USERS)
    logging.info(f"Users loaded: {len(users)}")

    logging.info("Connecting to PROJECT DB...")
    with psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB2, user=PG_USER, password=PG_PASSWORD
    ) as conn_proj:
        projects = exec_query(conn_proj, QUERY_PROJECTS)
    logging.info(f"Projects loaded: {len(projects)}")

    logging.info("Opening BK SQLite database...")
    conn_bk = sqlite3.connect(BK_SQLITE_PATH)
    conn_bk.row_factory = sqlite3.Row
    bk_rows = conn_bk.execute("SELECT * FROM Users").fetchall()
    conn_bk.close()

    bk_users = [dict(r) for r in bk_rows]
    logging.info(f"BK users loaded: {len(bk_users)}")

    bk_emails = {u.get("Email", "").strip().lower(): u for u in bk_users}

    matched_bk_users = []

    logging.info("Matching users...")

    for u in users:
        email = (u["Email"] or "").strip().lower()
        display = f"{u['LastName']} {u['FirstName']} {u['MiddleName']}".strip()

        if classify_tech_account(display):
            u["Status"] = "Tech"
            logging.info(f"Tech: {u['UserName']}")
            continue

        if not email:
            u["Status"] = "Fired"
            logging.info(f"No email, Fired: {u['UserName']}")
            continue

        if email in bk_emails:
            u["Status"] = "Active"
            matched_bk_users.append(bk_emails[email])
            logging.info(f"Active (in BK): {email}")
        else:
            u["Status"] = "Fired"
            logging.info(f"Fired (not in BK): {email}")

    logging.info(f"Matched BK users total: {len(matched_bk_users)}")

    workbook = xlsxwriter.Workbook("testIt_report.xlsx")

    write_sheet(workbook, "Users", users)
    write_sheet(workbook, "BK_Users", matched_bk_users)
    write_sheet(workbook, "Projects", projects)
    write_summary(workbook, users, projects)

    logging.info("Writing Tech_And_Fired sheet...")

    sheet = workbook.add_worksheet("Tech_And_Fired")
    tech_users = [u for u in users if u.get("Status") == "Tech"]
    fired_users = [u for u in users if u.get("Status") == "Fired"]
    headers = list(users[0].keys()) if users else []

    row = 0
    sheet.write(row, 0, "Технические учётки")
    row += 2

    if tech_users and headers:
        for col, h in enumerate(headers):
            sheet.write(row, col, h)
        row += 1
        for u in tech_users:
            for col, h in enumerate(headers):
                sheet.write(row, col, str(u[h]) if u[h] is not None else "")
            row += 1
    else:
        sheet.write(row, 0, "Нет данных")
        row += 2

    row += 1
    sheet.write(row, 0, "Уволенные")
    row += 2

    if fired_users and headers:
        for col, h in enumerate(headers):
            sheet.write(row, col, h)
        row += 1
        for u in fired_users:
            for col, h in enumerate(headers):
                sheet.write(row, col, str(u[h]) if u[h] is not None else "")
            row += 1
    else:
        sheet.write(row, 0, "Нет данных")
        row += 1

    workbook.close()
    logging.info("Excel report saved: testIt_report.xlsx")


if __name__ == "__main__":
    main()
