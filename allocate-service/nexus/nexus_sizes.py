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

    cols = ["repository_name", "format", "repository_type", "blob_store_name", "cleanup_policy"]
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

            try:
                cur.execute(q)
                repo_sizes.update(dict(cur.fetchall()))
            except psycopg2.Error:
                continue

        return repo_sizes

    conn = _conn()
    try:
        with conn.cursor() as cur:
            res = _exec(cur)
        conn.commit()
        return res
    finally:
        conn.close()


def _table_exists(cur, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM pg_catalog.pg_tables WHERE tablename = %s;",
        (table_name,),
    )
    return cur.fetchone() is not None


def _column_exists(cur, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s;
        """,
        (table_name, column_name),
    )
    return cur.fetchone() is not None


def get_kimb_top_folder_sizes(repo_name: str, log=None):
    repo_name = (repo_name or "").strip()
    if not repo_name:
        return {}

    def _exec(cur):
        cur.execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename LIKE %s;",
            ("%_content_repository",),
        )
        table_names = [x[0] for x in cur.fetchall()]

        out = {}

        if log:
            log.info("KIMB DB scan: try content tables=%d", len(table_names))

        for table in table_names:
            repo_type = table.replace("_content_repository", "")

            blob_tbl = f"{repo_type}_asset_blob"
            asset_tbl = f"{repo_type}_asset"
            cr_tbl = f"{repo_type}_content_repository"

            if not _table_exists(cur, blob_tbl) or not _table_exists(cur, asset_tbl) or not _table_exists(cur, cr_tbl):
                if log:
                    log.warning(
                        "KIMB DB skip repo_type=%s reason=missing_tables blob=%s asset=%s cr=%s",
                        repo_type,
                        blob_tbl,
                        asset_tbl,
                        cr_tbl,
                    )
                continue

            if not _column_exists(cur, asset_tbl, "path"):
                if log:
                    log.warning(
                        "KIMB DB skip repo_type=%s reason=no_asset_path_column asset_tbl=%s",
                        repo_type,
                        asset_tbl,
                    )
                continue

            q = sql.SQL("""
                SELECT
                    CASE
                        WHEN POSITION('/' IN asset.path) > 0 THEN SPLIT_PART(asset.path, '/', 1)
                        ELSE asset.path
                    END AS top_folder,
                    SUM(blob.blob_size) AS total_bytes
                FROM {blob_tbl} AS blob
                JOIN {asset_tbl} AS asset ON blob.asset_blob_id = asset.asset_blob_id
                JOIN {cr_tbl} AS content_repo ON content_repo.repository_id = asset.repository_id
                JOIN repository r ON content_repo.config_repository_id = r.id
                WHERE r.name = %s
                GROUP BY top_folder;
            """).format(
                blob_tbl=sql.Identifier(blob_tbl),
                asset_tbl=sql.Identifier(asset_tbl),
                cr_tbl=sql.Identifier(cr_tbl),
            )

            try:
                cur.execute(q, (repo_name,))
                rows = cur.fetchall()
            except psycopg2.Error as e:
                if log:
                    log.warning(
                        "KIMB DB query failed repo_type=%s error=%s",
                        repo_type,
                        str(e).strip().replace("\n", " "),
                    )
                continue

            if log:
                log.info("KIMB DB repo_type=%s rows=%d", repo_type, len(rows))

            for top_folder, total_bytes in rows:
                if not top_folder:
                    continue
                key = str(top_folder)
                out[key] = out.get(key, 0) + int(total_bytes or 0)

        return out

    conn = _conn()
    try:
        with conn.cursor() as cur:
            res = _exec(cur)
        conn.commit()
        return res
    finally:
        conn.close()