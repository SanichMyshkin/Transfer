import logging
from psycopg2 import sql

from database.connection import get_db_connection

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(module)s - %(message)s"
)
logger = logging.getLogger(__name__)

def get_repository_sizes() -> dict:
    """Функция для вычисления размера репозиториев"""
    logger.info("🚀 Начало подсчета размера репозиториев")

    repo_sizes = {}
    conn = None

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename LIKE %s;",
                ("%_content_repository",),
            )
            table_names = [x[0] for x in cur.fetchall()]
            logger.info(f"🔍 Найдено {len(table_names)} таблиц content_repository")
            for table in table_names:
                repo_type = table.replace("_content_repository", "")
                logger.info(f"📦 Обработка репозитория типа: {repo_type}")

                query = sql.SQL(
                    """
                    SELECT r.name, SUM(blob_size)
                    FROM {} AS blob
                    JOIN {} AS asset ON blob.asset_blob_id = asset.asset_blob_id
                    JOIN {} AS content_repo ON content_repo.repository_id = asset.repository_id
                    JOIN repository r ON content_repo.config_repository_id = r.id
                    GROUP BY r.name;
                    """
                ).format(
                    sql.Identifier(f"{repo_type}_asset_blob"),
                    sql.Identifier(f"{repo_type}_asset"),
                    sql.Identifier(f"{repo_type}_content_repository"),
                )

                try:
                    cur.execute(query)
                    rows = cur.fetchall()
                    logger.info(f"🔹 Найдено {len(rows)} записей для типа {repo_type}")
                    repo_sizes.update(dict(rows))
                except Exception as query_err:
                    logger.error(
                        f"❌ Ошибка при запросе данных для {repo_type}: {query_err}",
                        exc_info=True,
                    )

            if repo_sizes:
                total_size = sum(repo_sizes.values())
                for name, size_bytes in repo_sizes.items():
                    logger.info(f"{name}: {size_bytes}")
                logging.info(f"🧮 Общий размер всех репозиториев: {total_size}")
            else:
                logger.warning("⚠️ Репозитории не найдены или их размер равен 0.")

    except Exception as e:
        logger.error(f"❌ Ошибка при получении размеров репозиториев: {e}")
    finally:
        if conn:
            conn.close()

    return repo_sizes


def get_repository_data() -> list:
    """Функция для получения информации о политиках очистки репозиториев"""
    logger.info("🚀 Получение информации о политиках очистки")

    results = []
    conn = None

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            query = """
                SELECT 
                    r.name AS repository_name,
                    SPLIT_PART(r.recipe_name, '-', 1) AS format,
                    SPLIT_PART(r.recipe_name, '-', 2) AS repository_type,
                    r.attributes->'storage'->>'blobStoreName' AS blob_store_name,
                    COALESCE(r.attributes->'cleanup'->>'policyName', '') AS cleanup_policy
                FROM 
                    repository r
                ORDER BY 
                    format, repository_type, repository_name;
            """
            cur.execute(query)
            rows = cur.fetchall()

            if not rows:
                logger.warning("⚠️ Не найдено политик очистки или репозиториев")
            else:
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in rows]
                logger.info(f"📋 Получена информация по {len(results)} репозиториям")

    except Exception as e:
        logger.error(f"❌ Ошибка при получении политик репозиториев: {e}")
    finally:
        if conn:
            conn.close()

    return results
