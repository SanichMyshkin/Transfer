import logging
import psycopg2
from psycopg2 import sql
import requests
import humanize

from config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASS,
    NEXUS_URL,
    NEXUS_USER,
    NEXUS_PASS,
)

logger = logging.getLogger("nexus_api")


def pg_connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def pg_execute_custom(fn):
    with pg_connect() as conn:
        with conn.cursor() as cur:
            try:
                result = fn(cur)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise


def get_repository_sizes():
    logger.info("=== Получение размеров репозиториев ===")

    def _exec(cur):
        cur.execute(
            """
            SELECT tablename
            FROM pg_catalog.pg_tables
            WHERE tablename LIKE %s;
            """,
            ("%_content_repository",),
        )

        table_names = [row[0] for row in cur.fetchall()]
        logger.info(f"Найдено таблиц форматов: {len(table_names)}")

        repo_sizes = {}

        for table in table_names:
            repo_type = table.replace("_content_repository", "")
            logger.info(f"→ Обработка формата: {repo_type}")

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
            rows = cur.fetchall()

            for repo_name, size in rows:
                size = size or 0
                repo_sizes[repo_name] = {
                    "size_bytes": size,
                    "size_human": humanize.naturalsize(size, binary=True),
                }

        logger.info("Размеры репозиториев собраны.")
        return repo_sizes

    return pg_execute_custom(_exec)


def nexus_session():
    s = requests.Session()
    s.auth = (NEXUS_USER, NEXUS_PASS)
    return s


def get_roles():
    url = f"{NEXUS_URL}/service/rest/v1/security/roles"
    logger.info("Получаем роли Nexus…")

    resp = nexus_session().get(url)
    resp.raise_for_status()
    roles = resp.json()

    logger.info(f"Получено {len(roles)} ролей.")
    return roles


def extract_ad_group_repo_mapping(roles):
    logger.info("=== Извлекаем default AD-группы с репозиториями ===")
    mappings = []
    for role in roles:
        if role.get("source") != "default":
            continue
        ad_group = role["id"]
        if ad_group.startswith("nx-admin") or ad_group.startswith("nx-anonymous"):
            continue
        privileges = role.get("privileges", [])
        repos = set()
        for p in privileges:
            if not p.startswith("nx-repository-"):
                continue
            parts = p.split("-")
            if len(parts) < 6:
                continue
            repo_name = "-".join(parts[4:-1])
            repos.add(repo_name)
        for repo in sorted(repos):
            mappings.append(
                {
                    "ad_group": ad_group,
                    "repository": repo,
                }
            )
    logger.info(f"AD-групп с репозиториями: {len({m['ad_group'] for m in mappings})}")
    logger.info(f"Всего связок AD → repo: {len(mappings)}")

    return mappings


def extract_all_default_groups(roles):
    logger.info("=== Извлекаем ВСЕ default AD-группы ===")

    groups = set()

    for role in roles:
        if role.get("source") != "default":
            continue

        group = role["id"]
        if group.startswith("nx-admin") or group.startswith("nx-anonymous"):
            continue

        groups.add(group)

    logger.info(f"Всего default AD-групп: {len(groups)}")
    return sorted(groups)
