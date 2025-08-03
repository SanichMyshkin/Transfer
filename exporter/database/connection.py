# db/connection.py
import psycopg2
from urllib.parse import urlparse
import logging

from config import DATABASE_URL

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(module)s - %(message)s"
)
logger = logging.getLogger(__name__)


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
        logger.error(f"Не удалось подключиться к БД: {e}")
        raise
