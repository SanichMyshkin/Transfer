import logging
from database.connection import get_db_connection


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(module)s - %(message)s"
)
logger = logging.getLogger(__name__)


def fetch_docker_tags_data():
    """
    Получает информацию о docker-образах и их тегах из БД
    """
    conn = None
    result = []

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    dc.name,
                    dc.version,
                    r.name,
                    r.recipe_name,
                    (r.attributes::jsonb -> 'storage' ->> 'blobStoreName')
                FROM
                    docker_component dc
                JOIN
                    docker_content_repository dcr ON dc.repository_id = dcr.repository_id
                JOIN
                    repository r ON dcr.config_repository_id = r.id;
            """)
            result = cur.fetchall()

    except Exception as e:
        logger.error(f"❌ Ошибка при выполнении запроса docker tags: {e}")
    finally:
        if conn:
            conn.close()

    return result
