import logging
import psycopg2
from psycopg2 import sql
import requests
import humanize
from credentials.config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS,
    NEXUS_URL, NEXUS_USER, NEXUS_PASS,
)

logger = logging.getLogger("nexus_api")


def pg_connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def pg_execute_custom(fn):
    with pg_connect() as conn:
        with conn.cursor() as cur:
            result = fn(cur)
            conn.commit()
            return result


def get_repository_sizes():
    logger.info("Получение размеров репозиториев")

    def _exec(cur):
        cur.execute(
            'SELECT tablename FROM pg_catalog.pg_tables WHERE tablename LIKE %s;',
            ("%_content_repository",),
        )
        table_names = [row[0] for row in cur.fetchall()]

        repo_sizes = {}

        for table in table_names:
            repo_type = table.replace("_content_repository", "")

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
            for repo_name, size in cur.fetchall():
                size = size or 0
                repo_sizes[repo_name] = {
                    "size_bytes": size,
                    "size_human": humanize.naturalsize(size, binary=True),
                }

        return repo_sizes

    return pg_execute_custom(_exec)


def nexus_session():
    s = requests.Session()
    s.auth = (NEXUS_USER, NEXUS_PASS)
    return s


def get_roles():
    url = f"{NEXUS_URL}/service/rest/v1/security/roles"
    resp = nexus_session().get(url)
    resp.raise_for_status()
    return resp.json()


def extract_ad_group_repo_mapping(roles):
    mappings = []

    for role in roles:
        if role.get("source") != "default":
            continue

        ad_group = role["id"]
        if ad_group.startswith("nx-admin") or ad_group.startswith("nx-anonymous"):
            continue

        repos = set()
        for p in role.get("privileges", []):
            if p.startswith("nx-repository-"):
                parts = p.split("-")
                if len(parts) >= 6:
                    repo_name = "-".join(parts[4:-1])
                    repos.add(repo_name)

        for repo in repos:
            mappings.append({"ad_group": ad_group, "repository": repo})

    return mappings


def extract_all_default_groups(roles):
    groups = set()
    for role in roles:
        if role.get("source") != "default":
            continue
        group = role["id"]
        if not group.startswith(("nx-admin", "nx-anonymous")):
            groups.add(group)
    return sorted(groups)


def invert_repo_mapping(mappings):
    repo_to_groups = {}
    for m in mappings:
        repo = m["repository"]
        ad = m["ad_group"]
        repo_to_groups.setdefault(repo, set()).add(ad)
    return repo_to_groups


def build_final_repo_table():
    roles = get_roles()
    mappings = extract_ad_group_repo_mapping(roles)
    repo_sizes = get_repository_sizes()
    repo_to_groups = invert_repo_mapping(mappings)

    final = []

    for repo, size_info in repo_sizes.items():
        ad_groups = repo_to_groups.get(repo, set())

        final.append({
            "repository": repo,
            "ad_groups": ", ".join(sorted(ad_groups)) if ad_groups else "",
            "size_human": size_info["size_human"],
            "size_bytes": size_info["size_bytes"],
        })

    final.sort(key=lambda x: x["repository"])
    return final
