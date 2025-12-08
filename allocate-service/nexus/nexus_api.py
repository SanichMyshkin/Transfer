# nexus_api.py
import requests
from psycopg2 import sql
from database.utils.query_to_db import execute_custom, fetch_data
from common.logs import logging
from config import NEXUS_URL, NEXUS_USER, NEXUS_PASS

logger = logging.getLogger("nexus_api")


# ============================================================
# Nexus API сессия
# ============================================================


def nexus_session():
    session = requests.Session()
    session.auth = (NEXUS_USER, NEXUS_PASS)
    return session


# ============================================================
# Размеры репозиториев (PostgreSQL)
# ============================================================


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
        cur.execute(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename LIKE %s;",
            ("%_content_repository",),
        )
        table_names = [row[0] for row in cur.fetchall()]
        repo_sizes = {}

        for table in table_names:
            repo_type = table.replace("_content_repository", "")
            logger.info(f"📦 Сканируем репозиторий типа: {repo_type}")

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


# ============================================================
# Общие данные всех репозиториев
# ============================================================


def get_repository_data():
    """
    Возвращает список словарей:
    [
        {
            "repository_name": "...",
            "format": "maven",
            "repository_type": "hosted",
            "blob_store_name": "...",
            "cleanup_policy": "...",
        },
        ...
    ]
    """

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
    columns = [
        "repository_name",
        "format",
        "repository_type",
        "blob_store_name",
        "cleanup_policy",
    ]
    return [dict(zip(columns, row)) for row in rows]


# ============================================================
# Роли Nexus (REST API)
# ============================================================


def get_roles():
    """
    Возвращает список ролей:
    [
        {
            "id": "...",
            "name": "...",
            "source": "LDAP" | "default",
            "privileges": [...],
            "roles": [...]
        }
    ]
    """
    url = f"{NEXUS_URL}/service/rest/v1/security/roles"
    session = nexus_session()
    resp = session.get(url)
    resp.raise_for_status()
    return resp.json()


# ============================================================
# Извлечение AD-групп из ролей
# ============================================================


def get_ad_groups_from_roles(roles):
    """
    Возвращает:
    {
        "role_id": "CN=Group,OU=Groups,...",
        ...
    }
    """
    ad_map = {}

    for r in roles:
        if r.get("source") == "LDAP":
            ad_map[r["id"]] = r["id"]

    return ad_map


# ============================================================
# Определение репозиториев по ролям Nexus
# ============================================================


def map_roles_to_repositories(roles):
    """
    Анализ привилегий роли:
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
                repo = parts[3]
                repos.add(repo)

        if repos:
            mapping[r["id"]] = sorted(repos)

    return mapping
