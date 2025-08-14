import javaobj.v2 as javaobj
from javaobj.v2.beans import JavaInstance, JavaField
from database.utils.query_to_db import fetch_data
from common.logs import logging


logging.getLogger("javaobj.parser").setLevel(logging.WARNING)


def get_jobs_data():
    rows = fetch_data("SELECT job_data FROM qrtz_job_details ORDER BY job_name")
    result = []
    for idx, (job_data_bytes,) in enumerate(rows, start=1):
        try:
            if job_data_bytes:
                raw_obj = javaobj.loads(job_data_bytes)
                parsed_data = convert_java(raw_obj)
                if parsed_data:
                    result.append(parsed_data)
        except Exception as e:
            logging.error(f"[{idx}] Ошибка при парсинге job_data: {e}")
    logging.info(f"Получено задач: {len(result)}")
    return result


def convert_java(obj):
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
