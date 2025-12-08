import logging
import psycopg2
from psycopg2 import sql
import requests

from config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS,
    NEXUS_URL, NEXUS_USER, NEXUS_PASS
)

logger = logging.getLogger("nexus_api")


# ======================================================
# PostgreSQL utilities (with context manager)
# ======================================================

def pg_connect():
    """Создание подключения к PostgreSQL."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


def pg_query(query, params=None):
    """Универсальная простая выборка SELECT."""
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchall()


def pg_execute_custom(fn):
    """
    Выполняет переданную функцию с курсором в контексте транзакции.
    Используется для сложных SQL конструкций.
    """
    with pg_connect() as conn:
        with conn.cursor() as cur:
            try:
                result = fn(cur)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise


# ======================================================
# Получение размеров репозиториев Nexus
# ======================================================

def get_repository_sizes():
    """
    Возвращает dict:
        {
            "maven-releases": 123456789,
            "docker-hosted": 987654321,
            ...
        }
    """

    def _exec(cur):
        cur.execute("""
            SELECT tablename
            FROM pg_catalog.pg_tables
            WHERE tablename LIKE %s;
        """, ("%_content_repository",))

        table_names = [row[0] for row in cur.fetchall()]
        repo_sizes = {}

        for table in table_names:
            repo_type = table.replace("_content_repository", "")
            logger.info(f"📦 Сканируем тип репозитория: {repo_type}")

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
                sql.Identifier(f"{repo_type}_content_repository")
            )

            cur.execute(query)
            for repo_name, size in cur.fetchall():
                repo_sizes[repo_name] = size

        return repo_sizes

    return pg_execute_custom(_exec)


# ======================================================
# Общая информация о репозиториях
# ======================================================

def get_repository_data():
    rows = pg_query("""
        SELECT 
            r.name AS repository_name,
            SPLIT_PART(r.recipe_name, '-', 1) AS format,
            SPLIT_PART(r.recipe_name, '-', 2) AS repository_type,
            r.attributes->'storage'->>'blobStoreName' AS blob_store_name,
            COALESCE(r.attributes->'cleanup'->>'policyName', '') AS cleanup_policy
        FROM repository r
        ORDER BY format, repository_type, repository_name;
    """)

    columns = ["repository_name", "format", "repository_type", "blob_store_name", "cleanup_policy"]
    return [dict(zip(columns, row)) for row in rows]


# ======================================================
# Nexus API (roles, privileges, AD groups)
# ======================================================

def nexus_session():
    s = requests.Session()
    s.auth = (NEXUS_USER, NEXUS_PASS)
    return s


def get_roles():
    """
    Возвращает список ролей Nexus:
    [
        {
            id: "...",
            name: "...",
            source: "LDAP" | "default",
            privileges: [...],
            roles: [...]
        }
    ]
    """
    url = f"{NEXUS_URL}/service/rest/v1/security/roles"
    logger.info("Получаем роли Nexus...")
    session = nexus_session()
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def get_ad_groups_from_roles(roles):
    """
    Если роль LDAP → её id = DN группы.
    Возвращает:
    { "role_id": "CN=Group,OU=..." }
    """
    ad_map = {}
    for r in roles:
        if r.get("source") == "LDAP":
            ad_map[r["id"]] = r["id"]
    return ad_map


def map_roles_to_repositories(roles):
    """
    Ищем привилегии вида:
    nx-repository-view-<format>-<repo>-<action>

    Возвращает:
    {
        "role_id": ["repo1", "repo2", ...]
    }
    """
    mapping = {}

    for r in roles:
        repos = set()

        for p in r.get("privileges", []):
            parts = p.split("-")
            if len(parts) >= 5 and parts[1] == "repository":
                repo_name = parts[3]
                repos.add(repo_name)

        if repos:
            mapping[r["id"]] = sorted(repos)

    return mapping
