import os
import base64
import logging
import urllib3

import gitlab
import yaml
from dotenv import load_dotenv
from gitlab.exceptions import GitlabGetError

load_dotenv()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GITLAB_GROUP_ID = os.getenv("GITLAB_GROUP_ID")
GITLAB_FILE_PATH = os.getenv("GITLAB_FILE_PATH")
GITLAB_REF = os.getenv("GITLAB_REF", "main")


def get_gitlab_client():
    if not GITLAB_URL or not GITLAB_TOKEN:
        raise RuntimeError("Missing GITLAB_URL or GITLAB_TOKEN")
    return gitlab.Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN, ssl_verify=False)


def iter_group_projects(gl):
    group = gl.groups.get(GITLAB_GROUP_ID)
    return group.projects.list(all=True, include_subgroups=True, iterator=True)


def try_get_file_text(project):
    try:
        f = project.files.get(file_path=GITLAB_FILE_PATH, ref=GITLAB_REF)
    except GitlabGetError as e:
        if getattr(e, "response_code", None) == 404:
            return None
        raise
    return base64.b64decode(f.content).decode("utf-8", errors="replace")


def as_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        return [x]
    return [x]


def extract_labels(doc):
    if not isinstance(doc, dict):
        return [], 0

    jenkins = doc.get("jenkins")
    if not isinstance(jenkins, dict):
        return [], 0

    nodes = jenkins.get("nodes")
    nodes_list = list(nodes.values()) if isinstance(nodes, dict) else as_list(nodes)

    labels = set()

    for node in nodes_list:
        if not isinstance(node, dict):
            continue

        permanent = node.get("permanent")
        if isinstance(permanent, dict):
            ls = permanent.get("labelString")
            if isinstance(ls, str) and ls.strip():
                labels.add(ls.strip())

        # на всякий случай: если вдруг labelString окажется не только в permanent
        ls2 = node.get("labelString")
        if isinstance(ls2, str) and ls2.strip():
            labels.add(ls2.strip())

    return sorted(labels), len(nodes_list)


def collect_node():
    gl = get_gitlab_client()
    result = {}

    for p in iter_group_projects(gl):
        project = gl.projects.get(p.id)
        log.info(f"Project: {project.path}")

        text = try_get_file_text(project)
        if not text:
            log.info("  file not found")
            continue

        log.info("  file found")

        try:
            doc = yaml.safe_load(text)
        except yaml.YAMLError:
            log.warning("  invalid yaml")
            continue

        labels, nodes_count = extract_labels(doc)
        log.info(f"  nodes: {nodes_count}")

        if labels:
            log.info(f"  labels: {labels}")
            result[project.path] = labels
        else:
            log.info("  labels not found")

    return result
