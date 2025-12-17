import psycopg2
from psycopg2.extras import RealDictCursor
import os

AUTH_DB = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DB"),  # authdb
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
}

TESTIT_DB = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DB2"),  # testitdb
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
}


def fetch(conn_cfg, query):
    with psycopg2.connect(**conn_cfg) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            return cur.fetchall()


USERS_QUERY = """
SELECT
    u."Id",
    u."UserName",
    u."Email"
FROM "AspNetUsers" u;
"""

ROLES_QUERY = """
SELECT
    ur."UserId",
    r."Name" AS role_name
FROM "UserRoles" ur
JOIN "AspNetRoles" r ON r."Id" = ur."RoleId";
"""


PROJECTS_QUERY = """
SELECT
    "Id",
    "Name",
    "CreatedBy"
FROM "Projects";
"""


def main():
    users = fetch(AUTH_DB, USERS_QUERY)
    roles = fetch(AUTH_DB, ROLES_QUERY)
    projects = fetch(TESTIT_DB, PROJECTS_QUERY)

    # --- индексы ---
    users_by_id = {u["Id"]: u for u in users}

    roles_by_user = {}
    for r in roles:
        roles_by_user.setdefault(r["UserId"], []).append(r["role_name"])

    # --- результат ---
    result = []

    for p in projects:
        owner = users_by_id.get(p["CreatedBy"])
        owner_roles = roles_by_user.get(p["CreatedBy"], [])

        result.append(
            {
                "project_id": p["Id"],
                "project_name": p["Name"],
                "owner_username": owner["UserName"] if owner else None,
                "owner_email": owner["Email"] if owner else None,
                "owner_roles": owner_roles,
            }
        )

    for r in result:
        print(
            f"{r['project_name']} | "
            f"{r['owner_username']} | "
            f"roles={','.join(r['owner_roles'])}"
        )


if __name__ == "__main__":
    main()
