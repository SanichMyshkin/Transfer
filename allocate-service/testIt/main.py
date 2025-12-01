import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


DB_HOST = os.getenv("PG_HOST")
DB_PORT = os.getenv("PG_PORT")
DB_NAME = os.getenv("PG_DB")
DB_USER = os.getenv("PG_USER")
DB_PASSWORD = os.getenv("PG_PASSWORD")


QUERY_USERS = """
SELECT
    u."Id" AS user_id,
    u."UserName",
    u."FirstName",
    u."MiddleName",
    u."LastName",
    u."Email",
    u."CreatedBy",
    u."UserType",
    u."DistinguishedName",
    ARRAY_AGG(DISTINCT g."Name") AS group_names,
    ARRAY_AGG(DISTINCT r."Name") AS role_names
FROM "AspNetUsers" u
JOIN "UserPermissions" up ON u."Id" = up."UserId"
LEFT JOIN "UserGroups" ug ON u."Id" = ug."UserId"
LEFT JOIN "Groups" g ON ug."GroupId" = g."Id"
LEFT JOIN "UserRoles" ur ON u."Id" = ur."UserId"
LEFT JOIN "AspNetRoles" r ON ur."RoleId" = r."Id"
GROUP BY
    u."Id", u."UserName", u."FirstName", u."MiddleName",
    u."LastName", u."Email", u."CreatedBy",
    u."UserType", u."DistinguishedName"
ORDER BY u."UserName";
"""


def exec_query(conn, query: str, params=None):
    """Выполняет SQL и возвращает данные."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params or {})
        rows = cur.fetchall()
        logging.info("Query returned %d rows", len(rows))
        return rows


def main():
    db_config = {
        "host": DB_HOST,
        "port": DB_PORT,
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
    }

    logging.info("Opening DB connection")

    # подключение открываем один раз
    with psycopg2.connect(**db_config) as conn:
        # пример выполнения одного запроса
        users = exec_query(conn, QUERY_USERS)
        logging.info("Loaded users: %d", len(users))

        # сюда потом добавятся другие запросы
        # data2 = exec_query(conn, QUERY_2)
        # data3 = exec_query(conn, QUERY_3)

        # ничего не делаем с данными — пока


if __name__ == "__main__":
    main()
