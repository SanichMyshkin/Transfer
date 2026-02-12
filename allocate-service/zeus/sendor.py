import os
import logging
from dotenv import load_dotenv
import gitlab
import yaml
import urllib3

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

GITLAB_URL = os.getenv("GITLAB_URL", "").rstrip("/")
TOKEN = os.getenv("TOKEN", "")
GROUP_ID = os.getenv("GROUP_ID", "").strip()
GIT_REF = os.getenv("GIT_REF", "main")


def gl_connect():
    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN, ssl_verify=False, timeout=60)
    gl.auth()
    return gl


def normalize_cipher_value(v):
    s = str(v or "").strip()
    if not s.startswith("{cipher}"):
        return ""
    s = s[len("{cipher}") :].strip()
    if not s:
        return ""
    for ch in s:
        if ch not in "0123456789abcdefABCDEF":
            return ""
    return s.lower()


def parse_yaml_with_heal(text, project, path):
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as e:
        log.warning(f"[{project}] {path} -> YAML parse error: {e}")
        log.warning(f"[{project}] {path} -> пробуем вылечить (замена TAB на пробелы)")

        healed = text.replace("\t", "  ")

        try:
            data = yaml.safe_load(healed)
            log.warning(f"[{project}] {path} -> успешно вылечили YAML")
            return data
        except yaml.YAMLError as e2:
            log.error(f"[{project}] {path} -> вылечить не удалось: {e2}")
            return None


def extract_cipher_hashes_from_yaml(text, project, path):
    data = parse_yaml_with_heal(text, project, path)
    if not data:
        return set()

    try:
        test_telegram = (
            data["zeus"]["monitoringProperties"]["vars"]["zeusmonitoring"]["custom"][
                "testTelegram"
            ]
        )
    except Exception:
        log.error(f"[{project}] {path} -> неправильная структура YAML")
        return set()

    if not isinstance(test_telegram, list):
        log.error(f"[{project}] {path} -> testTelegram не список")
        return set()

    result = set()

    for item in test_telegram:
        if not isinstance(item, dict):
            continue
        if "chatId" not in item:
            continue

        h = normalize_cipher_value(item["chatId"])
        if h:
            result.add(h)

    return result


def get_gitlab_cipher_map(gl):
    group = gl.groups.get(GROUP_ID)
    projects = group.projects.list(all=True, include_subgroups=True)

    cipher_map = {}

    for p in projects:
        proj = gl.projects.get(p.id)
        log.info(f"Обрабатываем проект: {proj.path_with_namespace}")

        try:
            tree = proj.repository_tree(ref=GIT_REF, recursive=True, all=True)
        except Exception as e:
            log.error(f"[{proj.path_with_namespace}] repository_tree error: {e}")
            continue

        for item in tree:
            if item.get("type") != "blob":
                continue

            name = (item.get("name") or "").lower()
            if not (name.endswith("-monitors.yml") or name.endswith("-monitors.yaml")):
                continue

            try:
                f = proj.files.get(file_path=item["path"], ref=GIT_REF)
                text = f.decode().decode("utf-8")
                hashes = extract_cipher_hashes_from_yaml(
                    text, proj.path_with_namespace, item["path"]
                )
                for h in hashes:
                    cipher_map.setdefault(h, set()).add(proj.path_with_namespace)
            except Exception as e:
                log.error(
                    f"[{proj.path_with_namespace}] ошибка чтения {item['path']}: {e}"
                )

    return {h: sorted(v) for h, v in cipher_map.items()}


def main():
    gl = gl_connect()
    cipher_map = get_gitlab_cipher_map(gl)

    print("\n=== Результат ===")
    for h in sorted(cipher_map.keys()):
        print(h)
        for p in cipher_map[h]:
            print(f"  {p}")


if __name__ == "__main__":
    main()
