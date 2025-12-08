import logging
import psycopg2
from psycopg2 import sql
import requests
import humanize

from config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS,
    NEXUS_URL, NEXUS_USER, NEXUS_PASS
)

logger = logging.getLogger("nexus_api")


# ============================================================
# PostgreSQL
# ============================================================

def pg_connect():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
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


# ============================================================
# Размеры репозиториев
# ============================================================

def get_repository_sizes():
    """
    Возвращает:
    {
        "repo_name": {
            "size_bytes": int,
            "size_human": "117.74 MB"
        }
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
            logger.info(f"Сканируем репозитории типа: {repo_type}")

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
                size = size or 0
                repo_sizes[repo_name] = {
                    "size_bytes": size,
                    "size_human": humanize.naturalsize(size)
                }

        return repo_sizes

    return pg_execute_custom(_exec)


# ============================================================
# Roles API
# ============================================================

def nexus_session():
    s = requests.Session()
    s.auth = (NEXUS_USER, NEXUS_PASS)
    return s


def get_roles():
    url = f"{NEXUS_URL}/service/rest/v1/security/roles"
    logger.info("Запрашиваем роли Nexus...")
    resp = nexus_session().get(url)
    resp.raise_for_status()
    return resp.json()


# ============================================================
# AD-группы → репозитории
# ============================================================

def extract_ad_group_repo_mapping(roles):
    """
    Возвращает список:
    [
        {"ad_group": "UNAITP-15473_SRE", "repository": "docker-test-minio"},
        ...
    ]

    Берём только source == "default".
    Репозитории достаём из привилегий вида:
    nx-repository-<perm>-<format>-<repo-name-with-dashes>-<action>
    """

    mappings = []

    for role in roles:
        if role.get("source") != "default":
            continue

        ad_group = role["id"]
        privileges = role.get("privileges", [])

        repos = set()

        for p in privileges:
            if not p.startswith("nx-repository-"):
                continue

            parts = p.split("-")
            # пример:
            # nx-repository-view-docker-docker-test-minio-*
            # 0: nx
            # 1: repository
            # 2: view
            # 3: docker
            # 4..-2: части имени репозитория
            # -1: action (*, read, write...)
            if len(parts) < 6:
                continue

            repo_name = "-".join(parts[4:-1])
            if not repo_name:
                continue

            repos.add(repo_name)

        for repo in sorted(repos):
            mappings.append({
                "ad_group": ad_group,
                "repository": repo
            })

    logger.info(f"Найдено {len(mappings)} связей AD-группа → репозиторий")
    return mappings
