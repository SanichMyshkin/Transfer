import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

AUTH_DB = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
}

TESTIT_DB = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DB2"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
}


def fetch(cfg, query):
    with psycopg2.connect(**cfg) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            return cur.fetchall()


# ------------------ QUERIES ------------------

PROJECTS_QUERY = """
SELECT "Id", "Name"
FROM "Projects";
"""

PROJECT_GROUPS_QUERY = """
SELECT "ProjectId", "GroupId"
FROM "GroupProjects";
"""

GROUP_USERS_QUERY = """
SELECT
    ug."GroupId",
    u."Id" AS user_id,
    u."UserName",
    u."Email"
FROM "UserGroups" ug
JOIN "AspNetUsers" u ON u."Id" = ug."UserId";
"""


def main():
    projects = fetch(TESTIT_DB, PROJECTS_QUERY)
    project_groups = fetch(TESTIT_DB, PROJECT_GROUPS_QUERY)
    group_users = fetch(AUTH_DB, GROUP_USERS_QUERY)

    # --- индексы ---
    groups_by_project = {}
    for pg in project_groups:
        groups_by_project.setdefault(pg["ProjectId"], set()).add(pg["GroupId"])

    users_by_group = {}
    for gu in group_users:
        users_by_group.setdefault(gu["GroupId"], []).append(gu)

    # --- результат ---
    print("\nPROJECT ACCESS MAP\n" + "-" * 70)

    for p in projects:
        project_id = p["Id"]
        project_name = p["Name"]

        users = []
        for gid in groups_by_project.get(project_id, []):
            users.extend(users_by_group.get(gid, []))

        unique_users = {
            u["user_id"]: u for u in users
        }.values()

        print(f"\n{project_name}")
        for u in unique_users:
            print(f"  - {u['UserName']} ({u['Email']})")


if __name__ == "__main__":
    main()
