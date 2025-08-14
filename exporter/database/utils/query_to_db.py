from common.logs import logging
from database.utils.connection import get_db_connection


def fetch_data(query: str, params=None):
    """Выполняет SELECT-запрос с логированием"""
    conn = None
    result = []
    if params:
        logging.info(f"Параметры: {params}")
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            result = cur.fetchall()
        logging.info(f"Получено строк: {len(result)}")
    except Exception as e:
        logging.error(f"Ошибка при выполнении запроса: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Соединение закрыто")
    return result


def execute_custom(exec_func):
    """
    Универсальный метод для сложных запросов (с psycopg2.sql или нестандартной логикой).
    exec_func — функция, которая принимает cursor и сама выполняет всё, что нужно.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            return exec_func(cur)
    except Exception as e:
        logging.error(f"Ошибка при выполнении кастомного запроса: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logging.info("Соединение закрыто")
