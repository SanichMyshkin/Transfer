import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

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
JOIN "AspNetRoles" r
    ON r."Id" = ur."RoleId";
"""

# TESTIT DB
PROJECTS_QUERY = """
SELECT
    "Id",
    "Name",
    "CreatedById"
FROM "Projects";
"""


def main():
    print("Loading users from AUTH DB...")
    users = fetch(AUTH_DB, USERS_QUERY)
    print(f"Users loaded: {len(users)}")

    print("Loading roles from AUTH DB...")
    roles = fetch(AUTH_DB, ROLES_QUERY)
    print(f"Roles loaded: {len(roles)}")

    print("Loading projects from TESTIT DB...")
    projects = fetch(TESTIT_DB, PROJECTS_QUERY)
    print(f"Projects loaded: {len(projects)}")

    users_by_id = {u["Id"]: u for u in users}

    roles_by_user = {}
    for r in roles:
        roles_by_user.setdefault(r["UserId"], []).append(r["role_name"])

    result = []

    for p in projects:
        owner_id = p["CreatedById"]

        owner = users_by_id.get(owner_id)
        owner_roles = roles_by_user.get(owner_id, [])

        result.append(
            {
                "project_id": p["Id"],
                "project_name": p["Name"],
                "owner_id": owner_id,
                "owner_username": owner["UserName"] if owner else None,
                "owner_email": owner["Email"] if owner else None,
                "owner_roles": owner_roles,
            }
        )

    print("\nPROJECT OWNERSHIP\n" + "-" * 60)
    for r in result:
        print(
            f"{r['project_name']:<30} | "
            f"{r['owner_username'] or 'UNKNOWN':<20} | "
            f"roles={','.join(r['owner_roles']) or '-'}"
        )


if __name__ == "__main__":
    main()
