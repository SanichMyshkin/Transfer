import os
import logging
import sqlite3
import pymssql
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename="sync.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

SQLITE_FILE = os.getenv("SQLITE_FILE")
SQLITE_TABLE = os.getenv("SQLITE_TABLE")
SOURCE_VIEW = os.getenv("SOURCE_VIEW")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))


def fetch_mssql_data():
    conn = pymssql.connect(
        server=os.getenv("MSSQL_SERVER"),
        user=os.getenv("MSSQL_USER"),
        password=os.getenv("MSSQL_PASSWORD"),
        database=os.getenv("MSSQL_DB"),
    )

    with conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {SOURCE_VIEW}")
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            return columns, rows


def load_into_sqlite(columns, rows):
    with sqlite3.connect(SQLITE_FILE) as conn:
        cur = conn.cursor()

        cur.execute(f"DELETE FROM {SQLITE_TABLE}")

        placeholders = ", ".join(["?"] * len(columns))
        collist = ", ".join(columns)
        insert_sql = f"INSERT INTO {SQLITE_TABLE} ({collist}) VALUES ({placeholders})"

        batch = []
        for row in rows:
            batch.append(tuple(row))

            if len(batch) >= BATCH_SIZE:
                cur.executemany(insert_sql, batch)
                batch = []

        if batch:
            cur.executemany(insert_sql, batch)

        conn.commit()


def main():
    logging.info("Начинаем загрузку")
    columns, rows = fetch_mssql_data()
    logging.info(f"Получено строк: {len(rows)}")
    load_into_sqlite(columns, rows)
    logging.info("Готово")


if __name__ == "__main__":
    main()
