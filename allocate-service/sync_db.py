import os
import logging
import sqlite3
import pyodbc
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(
    filename="sync.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

MSSQL_CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('MSSQL_SERVER')};"
    f"DATABASE={os.getenv('MSSQL_DB')};"
    f"UID={os.getenv('MSSQL_USER')};"
    f"PWD={os.getenv('MSSQL_PASSWORD')};"
)

SQLITE_FILE = os.getenv("SQLITE_FILE")
SQLITE_TABLE = os.getenv("SQLITE_TABLE")
SOURCE_VIEW = os.getenv("SOURCE_VIEW")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))


def fetch_mssql_data():
    with pyodbc.connect(MSSQL_CONN_STR) as conn:
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
    logging.info("Начинаем загрузку из MSSQL")

    columns, rows = fetch_mssql_data()
    logging.info(f"Получено {len(rows)} строк")

    logging.info("Обновляем SQLite")
    load_into_sqlite(columns, rows)

    logging.info("Готово. Таблица обновлена.")


if __name__ == "__main__":
    main()
