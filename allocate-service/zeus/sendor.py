import os
import logging
from dotenv import load_dotenv
import psycopg2
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

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
SINCE = os.getenv("SINCE", "2026-01-01 00:00:00")

GITLAB_URL = os.getenv("GITLAB_URL", "").rstrip("/")
TOKEN = os.getenv("TOKEN", "")
GROUP_ID = os.getenv("GROUP_ID", "").strip()
GIT_REF = os.getenv("GIT_REF", "main")


def gl_connect():
    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN, ssl_verify=False, timeout=60)
    gl.auth()
    return gl


def normalize_cipher_value(v, project, path):
    s = str(v or "").strip()

    if not s.startswith("{cipher}"):
        log.error(f"[{project}] {path} -> chatId без {{cipher}}: {s}")
        return ""

    s = s[len("{cipher}") :].strip()

    if not s:
        log.error(f"[{project}] {path} -> пустой hash")
        return ""

    for ch in s:
        if ch not in "0123456789abcdefABCDEF":
            log.error(f"[{project}] {path} -> не hex значение: {s}")
            return ""

    return s.lower()


def extract_cipher_hashes_from_yaml(text, project, path):
    try:
        data = yaml.safe_load(text)
    except Exception as e:
        log.error(f"[{project}] YAML parse error в {path}: {e}")
        return set()

    try:
        test_telegram = (
            data["zeus"]["monitoringProperties"]["vars"]["zeusmonitoring"]["custom"][
                "testTelegram"
            ]
        )
    except Exception:
        log.error(f"[{project}] неправильная структура YAML в {path}")
        return set()

    result = set()

    if not isinstance(test_telegram, list):
        log.error(f"[{project}] testTelegram не список в {path}")
        return set()

    for item in test_telegram:
        if not isinstance(item, dict):
            log.error(f"[{project}] testTelegram элемент не dict в {path}")
            continue

        if "chatId" not in item:
            log.error(f"[{project}] нет chatId в {path}")
            continue

        h = normalize_cipher_value(item["chatId"], project, path)
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
                    text,
                    proj.path_with_namespace,
                    item["path"],
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

    for h in sorted(cipher_map.keys()):
        print(h)
        for p in cipher_map[h]:
            print(f"  {p}")


if __name__ == "__main__":
    main()
