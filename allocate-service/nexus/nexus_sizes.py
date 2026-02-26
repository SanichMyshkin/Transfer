# nexus_sizes.py
import os
import psycopg2
from psycopg2 import sql


def _conn():
    host = os.getenv("DB_HOST", "").strip()
    port = int(os.getenv("DB_PORT", "5432"))
    name = os.getenv("DB_NAME", "").strip()
    user = os.getenv("DB_USER", "").strip()
    password = os.getenv("DB_PASS", "").strip()

    if not host or not name or not user:
        raise RuntimeError("DB_HOST, DB_NAME, DB_USER должны быть заданы")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=name,
        user=user,
        password=password,
    )


def get_repository_data():
    q = """
        SELECT
            r.name AS repository_name,
            SPLIT_PART(r.recipe_name, '-', 1) AS format,
            SPLIT_PART(r.recipe_name, '-', 2) AS repository_type,
            r.attributes->'storage'->>'blobStoreName' AS blob_store_name,
            COALESCE(r.attributes->'cleanup'->>'policyName', '') AS cleanup_policy
        FROM repository r
        ORDER BY format, repository_type, repository_name;
    """

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(q)
            rows = cur.fetchall()
    finally:
        conn.close()

    cols = [
        "repository_name",
        "format",
        "repository_type",
        "blob_store_name",
        "cleanup_policy",
    ]
    return [dict(zip(cols, row)) for row in rows]


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

            q = sql.SQL("""
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

            cur.execute(q)
            repo_sizes.update(dict(cur.fetchall()))

        return repo_sizes

    conn = _conn()
    try:
        with conn.cursor() as cur:
            res = _exec(cur)
        conn.commit()
        return res
    finally:
        conn.close()


def get_raw_top_folder_sizes(repo_name: str):
    q = """
        SELECT
            split_part(ltrim(a.path, '/'), '/', 1) AS folder,
            SUM(b.blob_size)::bigint AS bytes
        FROM raw_asset_blob b
        JOIN raw_asset a ON b.asset_blob_id = a.asset_blob_id
        JOIN raw_content_repository cr ON cr.repository_id = a.repository_id
        JOIN repository r ON cr.config_repository_id = r.id
        WHERE r.name = %s
        GROUP BY folder
        ORDER BY bytes DESC;
    """

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(q, (repo_name,))
            rows = cur.fetchall()
            return {str(folder or ""): int(size or 0) for folder, size in rows if str(folder or "").strip()}
    finally:
        conn.close()