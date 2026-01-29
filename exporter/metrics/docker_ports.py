import re
from typing import Dict, List, Optional, Set

from prometheus_client import Gauge

from common.logs import logging
from common.config import NEXUS_API_URL, GITLAB_TOKEN, GITLAB_URL
from metrics.utils.api import get_from_nexus
from metrics.utils.api_gitlab import get_gitlab_file_content, get_gitlab_connection


gl = get_gitlab_connection(GITLAB_URL, GITLAB_TOKEN)


def extract_ports(file_text: str) -> List[int]:
    matches = re.findall(r"-p\s+(\d+(?:-\d+)?):", file_text)
    ports: List[int] = []

    for m in matches:
        if "-" in m:
            start, end = map(int, m.split("-"))
            ports.extend(range(start, end + 1))
        else:
            ports.append(int(m))

    return sorted(set(p for p in ports if p != 8081))


def map_ports_to_endpoints(nginx_conf: str) -> Dict[int, List[str]]:
    port_endpoint: Dict[int, List[str]] = {}

    matches = re.findall(
        r"location\s+[~^=]*\s*([^\s{]+)\s*{[^}]*?proxy_pass\s+http://[^:]+:(\d+)(?:[^\s;]*)\s*;",
        nginx_conf,
        re.DOTALL,
    )

    for location_path, port in matches:
        port = int(port)
        port_endpoint.setdefault(port, []).append(location_path.strip())

    return port_endpoint


def _extract_repo_port(repo: dict) -> Optional[int]:
    p = repo.get("docker", {}).get("httpPort") or repo.get("docker", {}).get(
        "httpsPort"
    )
    if p:
        try:
            return int(p)
        except Exception:
            return None

    repo_url = repo.get("url", "") or ""
    m = re.search(r":(\d+)", repo_url)
    return int(m.group(1)) if m else None


docker_repo_port_gauge = Gauge(
    "docker_repository_port_info",
    "Информация о портах и удалённых адресах docker-репозиториев Nexus",
    ["repository_name", "http_port", "remote_url", "repo_type", "endpoint"],
)

docker_port_status_gauge = Gauge(
    "docker_port_status",
    "Занятость портов: 1 = занят, 0 = свободен",
    ["port"],
)


def set_gauge(gauge: Gauge, labels: Dict[str, str], value: int) -> None:
    try:
        gauge.labels(**labels).set(value)
    except Exception as e:
        logging.error(f"Ошибка при установке метрики {gauge._name}: {e}")


def get_docker_repositories(nexus_url: str, auth: tuple) -> List[dict]:
    try:
        repositories = get_from_nexus(nexus_url, "repositorySettings", auth=auth)
    except Exception as e:
        logging.error(f"Ошибка при обращении к API Nexus: {e}")
        return []

    if not repositories:
        return []

    return [
        r
        for r in repositories
        if r.get("format") == "docker" and r.get("type") in ["proxy", "hosted", "group"]
    ]


def fetch_docker_ports_metrics(docker_repos: List[dict]) -> None:
    if not docker_repos:
        logging.warning("API Nexus вернул пустой список docker-репозиториев.")
        return

    logging.info(f"Получено {len(docker_repos)} docker-репозиториев из Nexus API.")
    docker_repo_port_gauge.clear()

    nginx_conf = get_gitlab_file_content(
        GITLAB_URL,
        GITLAB_TOKEN,
        gl,
        file_path="servers/msk-osf01nexus/wdata/services/upstream.conf",
    )

    port_to_endpoints = map_ports_to_endpoints(nginx_conf)

    for repo in docker_repos:
        repo_name = repo.get("name", "unknown")
        repo_type = repo.get("type", "unknown").capitalize()
        repo_url = repo.get("url", "")

        http_port = _extract_repo_port(repo)
        remote_url = repo.get("proxy", {}).get("remoteUrl", "")

        endpoints = port_to_endpoints.get(http_port, [])
        if endpoints:
            clean_endpoints = [ep.replace("/v2", "") for ep in endpoints]
            final_endpoint = ", ".join(
                sorted(
                    {
                        f"{NEXUS_API_URL.rstrip('/')}/{ep.lstrip('/')}"
                        for ep in clean_endpoints
                    }
                )
            )
        else:
            final_endpoint = "unknown"

        logging.info(
            f"Repo: {repo_name} | Type: {repo_type} | Port: {http_port or '—'} | Endpoint: {final_endpoint}"
        )

        set_gauge(
            docker_repo_port_gauge,
            labels=dict(
                repository_name=repo_name,
                http_port=str(http_port) if http_port else "None",
                remote_url=remote_url if remote_url else "None",
                repo_type=repo_type,
                endpoint=final_endpoint,
            ),
            value=1,
        )

    logging.info("Метрики по docker-репозиториям успешно обновлены.")


def fetch_ports_status_metrics(docker_repos: List[dict]) -> None:
    try:
        raw_ports = get_gitlab_file_content(
            GITLAB_URL,
            GITLAB_TOKEN,
            gl,
            file_path="servers/msk-osf01nexus/wdata/services/nexus3_docker.sh",
        )
    except Exception as e:
        logging.error(f"Ошибка при получении портов из GitLab: {e}")
        return

    all_ports = extract_ports(raw_ports)

    busy_ports: Set[int] = set()
    busy_ports_by_repo: Dict[int, List[str]] = {}

    for repo in docker_repos:
        port = _extract_repo_port(repo)
        if not port:
            continue
        busy_ports.add(port)
        busy_ports_by_repo.setdefault(port, []).append(repo.get("name", "unknown"))

    docker_port_status_gauge.clear()

    for port in all_ports:
        status = 1 if port in busy_ports else 0

        if status == 1:
            repos = ", ".join(sorted(set(busy_ports_by_repo.get(port, []))))
            logging.info(f"Порт {port} | занят | repos: {repos}")
        else:
            logging.info(f"Порт {port} | свободен")

        set_gauge(docker_port_status_gauge, {"port": str(port)}, status)

    logging.info(
        f"Метрики занятости портов обновлены. "
        f"Всего портов: {len(all_ports)}, занятых: {len(busy_ports)}"
    )


def fetch_docker_ports(nexus_url: str, auth: tuple) -> None:
    docker_repos = get_docker_repositories(nexus_url, auth)
    fetch_docker_ports_metrics(docker_repos)
    fetch_ports_status_metrics(docker_repos)
