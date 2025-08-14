import psycopg2
from urllib.parse import urlparse
from database.utils.query_to_db import logging
from common.config import DATABASE_URL


def get_db_connection():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL не задан")

    db_params = urlparse(DATABASE_URL)

    try:
        conn = psycopg2.connect(
            host=db_params.hostname,
            database=db_params.path.lstrip("/"),
            user=db_params.username,
            password=db_params.password,
            port=db_params.port or 5432,
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"Не удалось подключиться к БД: {e}")
        raise
