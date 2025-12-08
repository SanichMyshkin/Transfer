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


# ============================================================
# PostgreSQL
# ============================================================


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

    Теперь подробно логируем ВСЕ шаги.
    """

    logger.info("=== Получение размеров репозиториев ===")

    def _exec(cur):
        logger.info("Запрос списка таблиц *_content_repository …")

        cur.execute(
            """
            SELECT tablename
            FROM pg_catalog.pg_tables
            WHERE tablename LIKE %s;
        """,
            ("%_content_repository",),
        )

        table_names = [row[0] for row in cur.fetchall()]

        logger.info(f"Найдено {len(table_names)} таблиц контента:")
        for t in table_names:
            logger.info(f"  - {t}")

        repo_sizes = {}

        for table in table_names:
            repo_type = table.replace("_content_repository", "")
            logger.info(f"→ Обработка формата: {repo_type}")

            # Генерация SQL запроса
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

            logger.info(f"SQL для формата {repo_type} сгенерирован, выполняем…")

            cur.execute(query)
            rows = cur.fetchall()

            logger.info(f"Получено {len(rows)} строк для формата {repo_type}")

            for repo_name, size in rows:
                if size is None:
                    logger.warning(
                        f"!!! Репозиторий {repo_name} имеет NULL size — записываем 0"
                    )
                    size = 0
                logger.info(
                    f"  Репозиторий {repo_name}: size = {size} bytes ({humanize.naturalsize(size)})"
                )

                repo_sizes[repo_name] = {
                    "size_bytes": size,
                    "size_human": humanize.naturalsize(size),
                }

        logger.info("=== Завершено получение размеров репозиториев ===")
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
    logger.info("Запрашиваем роли Nexus…")
    resp = nexus_session().get(url)
    resp.raise_for_status()
    logger.info(f"Получено {len(resp.json())} ролей")
    return resp.json()


# ============================================================
# AD-группы → репозитории (default roles)
# ============================================================


def extract_ad_group_repo_mapping(roles):
    """
    Возвращает список:
    [
        {"ad_group": "...", "repository": "..."},
        ...
    ]

    Фильтруем:
    - source == "default"
    - НЕ включаем nx-admin*
    - НЕ включаем nx-anonymous*
    """

    logger.info("=== Обрабатываем AD-группы ===")
    mappings = []

    for role in roles:
        source = role.get("source")
        if source != "default":
            continue

        ad_group = role["id"]

        # 🔥 СКИПАЕМ системные роли
        if ad_group.startswith("nx-admin") or ad_group.startswith("nx-anonymous"):
            logger.info(f"Пропускаем системную роль: {ad_group}")
            continue

        privileges = role.get("privileges", [])
        repos = set()

        logger.info(f"Роль AD: {ad_group}, привилегий: {len(privileges)}")

        for p in privileges:
            if not p.startswith("nx-repository-"):
                continue

            parts = p.split("-")
            if len(parts) < 6:
                logger.warning(f"Неполная привилегия: {p}")
                continue

            # Правильный разбор имени репо (учёт дефисов!)
            repo_name = "-".join(parts[4:-1])

            logger.info(f"  Привилегия: {p} → репозиторий: {repo_name}")

            repos.add(repo_name)

        for repo in sorted(repos):
            mappings.append({"ad_group": ad_group, "repository": repo})

    logger.info(f"=== Найдено {len(mappings)} связей AD → repo ===")
    return mappings
