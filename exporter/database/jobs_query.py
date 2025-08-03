# jobs_reader.py

import logging
import javaobj.v2 as javaobj
from javaobj.v2.beans import JavaInstance, JavaField
from database.connection import get_db_connection


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(module)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Отключаем логи от библиотеки javaobj
logging.getLogger("javaobj").setLevel(logging.WARNING)


def get_jobs_data():
    """
    Получает данные задач из БД и возвращает список job_data словарей
    :return: список словарей job_data
    """
    result = []

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT job_data
                    FROM qrtz_job_details 
                    ORDER BY job_name
                """)

                rows = cursor.fetchall()

                for idx, (job_data_bytes,) in enumerate(rows, start=1):
                    try:
                        if job_data_bytes:
                            raw_obj = javaobj.loads(job_data_bytes)
                            parsed_data = convert_java(raw_obj)
                            if parsed_data:
                                result.append(parsed_data)
                    except Exception as e:
                        logger.error(f"[{idx}] Ошибка при парсинге job_data: {e}")

        logger.info(f"Данные успешно собраны из БД. Получено задач: {len(result)}")

    except Exception as e:
        logger.error(f"Ошибка при получении данных из БД: {e}")
        raise

    return result


def convert_java(obj):
    """Рекурсивно преобразует Java-объект в Python-структуры"""
    if obj is None:
        return None

    if isinstance(obj, JavaField):
        return convert_java(getattr(obj, "field_value", None))

    if isinstance(obj, JavaInstance):
        class_name = getattr(obj.classdesc, "name", "")
        fields = getattr(obj, "field_data", {})

        if class_name == "org.quartz.JobDataMap":
            for field_value in fields.values():
                if isinstance(field_value, dict):
                    for sub_value in field_value.values():
                        if isinstance(sub_value, dict):
                            return {str(k): str(v) for k, v in sub_value.items()}
            return {}

        return {
            str(field_name): convert_java(field_obj)
            for field_name, field_obj in fields.items()
            if convert_java(field_obj) is not None
        }

    if isinstance(obj, dict):
        return {str(k): convert_java(v) for k, v in obj.items() if v is not None}

    return str(obj) if obj is not None else None
