import os
import base64
import gitlab

try:
    import tomllib
except ImportError:
    import tomli as tomllib


GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GITLAB_PROJECT_ID = os.getenv("GITLAB_PROJECT_ID", "3058")
GITLAB_FILE_PATH = os.getenv("GITLAB_FILE_PATH", "grafana_main/ldap.toml")
GITLAB_REF = os.getenv("GITLAB_REF", "main")


def _get_gitlab_project():
    if not GITLAB_URL or not GITLAB_TOKEN:
        raise RuntimeError("Missing GITLAB_URL or GITLAB_TOKEN")
    gl = gitlab.Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN, ssl_verify=False)
    return gl.projects.get(GITLAB_PROJECT_ID)


def _load_raw_text() -> str:
    project = _get_gitlab_project()
    f = project.files.get(file_path=GITLAB_FILE_PATH, ref=GITLAB_REF)
    decoded = base64.b64decode(f.content)
    return decoded.decode("utf-8")


def _extract_org_ids_from_data(data: dict) -> set:
    servers = data.get("servers")
    org_ids = set()

    mappings_raw = []

    if isinstance(servers, dict):
        gm = servers.get("group_mappings", [])
        if isinstance(gm, list):
            mappings_raw.extend(gm)
    elif isinstance(servers, list):
        for item in servers:
            if isinstance(item, dict):
                gm = item.get("group_mappings")
                if isinstance(gm, list):
                    mappings_raw.extend(gm)

    for m in mappings_raw:
        if not isinstance(m, dict):
            continue
        org_id = m.get("org_id")
        if org_id is not None:
            org_ids.add(org_id)

    return org_ids


def get_unique_org_ids() -> set:
    text = _load_raw_text()
    data = tomllib.loads(text)
    return _extract_org_ids_from_data(data)
