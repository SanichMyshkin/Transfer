from database.utils.query_to_db import fetch_data
from common.logs import logging

def fetch_docker_ports():
    query = """
        SELECT r.name, r.attributes
        FROM repository r
        WHERE r.recipe_name IN ('docker-hosted', 'docker-proxy');
    """
    rows = fetch_data(query)
    docker_repos_info = []
    for repo_name, attributes in rows:
        try:
            docker_attrs = attributes.get("docker", {})
            proxy_attrs = attributes.get("proxy", {})
            http_port = int(docker_attrs.get("httpPort")) if docker_attrs.get("httpPort") else None
            if http_port is None:
                logging.info(f"ℹ️ У репозитория '{repo_name}' не задан httpPort.")
            docker_repos_info.append({
                "repository_name": repo_name,
                "http_port": http_port,
                "remote_url": proxy_attrs.get("remoteUrl")
            })
        except Exception as parse_error:
            logging.warning(f"⚠️ Ошибка при обработке '{repo_name}': {parse_error}")
    return docker_repos_info
