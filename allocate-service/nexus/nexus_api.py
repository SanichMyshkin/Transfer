import logging
import psycopg2
from psycopg2 import sql
from config import PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE
import humanize


logger = logging.getLogger("nexus_api")


# ============================================================
# 1. Получение всех ролей из Nexus API
# ============================================================

def get_roles():
    from config import NEXUS_API_URL, NEXUS_USER, NEXUS_PASSWORD
    import requests

    logger.info("Запрашиваем роли из Nexus API...")

    url = f"{NEXUS_API_URL}/security/roles"
    resp = requests.get(url, auth=(NEXUS_USER, NEXUS_PASSWORD), timeout=30)

    resp.raise_for_status()
    roles = resp.json()

    logger.info(f"Получено ролей: {len(roles)}")

    return roles


# ============================================================
# 2. Правильная выборка: только роли с репозиториями
# ============================================================

def extract_ad_group_repo_mapping(roles):
    """
    Возвращает ТОЛЬКО те default роли, у которых есть привилегии nx-repository-*.
    Эти данные нужны для листа RepoUsage.
    """

    mappings = []

    for role in roles:
        if role.get("source") != "default":
            continue

        ad_group = role["id"]

        # пропускаем системные
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

        # ВАЖНО: сюда добавляем ТОЛЬКО роли с репозиториями
        for repo in sorted(repos):
            mappings.append({
                "ad_group": ad_group,
                "repository": repo
            })

    logger.info(f"AD групп с репозиториями: {len({m['ad_group'] for m in mappings})}")
    logger.info(f"Всего связок AD → repo: {len(mappings)}")

    return mappings


# ============================================================
# 3. Все default роли (для LDAP)
# ============================================================

def extract_all_default_groups(roles):
    """
    Возвращает ВСЕ default роли, кроме admin/anonymous.
    Эти группы пойдут в LDAP.
    """

    groups = set()

    for role in roles:
        if role.get("source") != "default":
            continue

        ad_group = role["id"]

        if ad_group.startswith("nx-admin") or ad_group.startswith("nx-anonymous"):
            continue

        groups.add(ad_group)

    logger.info(f"Всего default AD групп для LDAP: {len(groups)}")

    return sorted(groups)


# ============================================================
# 4. Получение размеров репозиториев
# ============================================================

def get_repository_sizes():
    logger.info("Подключаемся к PostgreSQL чтобы получить размеры репозиториев...")

    repo_sizes = {}

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DATABASE
    )

    with conn:
        with conn.cursor() as cur:

            # находим таблицы репозиториев
            cur.execute(
                "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename LIKE %s;",
                ("%_content_repository",)
            )
            table_names = [x[0] for x in cur.fetchall()]

            for table in table_names:
                repo_type = table.replace("_content_repository", "")

                logger.info(f"📦 Читаем репозитории типа: {repo_type}")

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
                    sql.Identifier(table),
                )

                cur.execute(query)

                for repo_name, size_bytes in cur.fetchall():
                    repo_sizes[repo_name] = humanize.naturalsize(size_bytes or 0, binary=True)

    logger.info(f"Размеры репозиториев получены: {len(repo_sizes)} шт.")

    return repo_sizes
