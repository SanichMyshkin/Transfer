# nexus_raw_folder_size.py
import os
import argparse
import logging
import psycopg2
from psycopg2 import sql


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("nexus_raw_folder_size")


def _conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    args = parser.parse_args()

    conn = _conn()
    try:
        with conn.cursor() as cur:
            q = """
                SELECT
                    COALESCE(NULLIF(split_part(a.path, '/', 1), ''), '<root>') AS folder,
                    SUM(b.blob_size)::bigint AS bytes
                FROM raw_asset_blob b
                JOIN raw_asset a ON b.asset_blob_id = a.asset_blob_id
                JOIN raw_content_repository cr ON cr.repository_id = a.repository_id
                JOIN repository r ON cr.config_repository_id = r.id
                WHERE r.name = %s
                GROUP BY folder
                ORDER BY bytes DESC;
            """
            cur.execute(q, (args.repo,))
            rows = cur.fetchall()

            for folder, size in rows:
                log.info("%s | %s", folder, size)

    finally:
        conn.close()


if __name__ == "__main__":
    main()