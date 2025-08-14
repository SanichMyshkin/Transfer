from psycopg2 import sql
from database.utils.query_to_db import execute_custom, fetch_data
from common.logs import logging

def get_repository_sizes():
    def _exec(cur):
        cur.execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename LIKE %s;",
            ("%_content_repository",),
        )
        table_names = [x[0] for x in cur.fetchall()]
        repo_sizes = {}
        for table in table_names:
            repo_type = table.replace("_content_repository", "")
            logging.info(f"📦 Обработка репозитория типа: {repo_type}")
            query = sql.SQL("""
                SELECT r.name, SUM(blob_size)
                FROM {} AS blob
                JOIN {} AS asset ON blob.asset_blob_id = asset.asset_blob_id
                JOIN {} AS content_repo ON content_repo.repository_id = asset.repository_id
                JOIN repository r ON content_repo.config_repository_id = r.id
                GROUP BY r.name;
            """).format(
                sql.Identifier(f"{repo_type}_asset_blob"),
                sql.Identifier(f"{repo_type}_asset"),
                sql.Identifier(f"{repo_type}_content_repository"),
            )
            cur.execute(query)
            repo_sizes.update(dict(cur.fetchall()))
        return repo_sizes
    return execute_custom(_exec)

def get_repository_data():
    query = """
        SELECT 
            r.name AS repository_name,
            SPLIT_PART(r.recipe_name, '-', 1) AS format,
            SPLIT_PART(r.recipe_name, '-', 2) AS repository_type,
            r.attributes->'storage'->>'blobStoreName' AS blob_store_name,
            COALESCE(r.attributes->'cleanup'->>'policyName', '') AS cleanup_policy
        FROM repository r
        ORDER BY format, repository_type, repository_name;
    """
    rows = fetch_data(query)
    columns = ["repository_name", "format", "repository_type", "blob_store_name", "cleanup_policy"]
    return [dict(zip(columns, row)) for row in rows]
