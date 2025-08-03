# спользует skopeo, но создаёт отдельный контейнер docker run на каждый образ

# import argparse
import logging
import subprocess
import requests
import urllib3
import sys
import os
from dotenv import load_dotenv


urllib3.disable_warnings()

# === Логирование ===
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


load_dotenv()

# === Чтение конфигурации из переменных окружения ===
NEXUS_URL = os.environ.get("NEXUS_URL")
USERNAME = os.environ.get("USERNAME")
PASSWORD = os.environ.get("PASSWORD")

if not all([NEXUS_URL, USERNAME, PASSWORD]):
    log.error(
        "❌ Не заданы переменные окружения: NEXUS_URL, NEXUS_USERNAME, NEXUS_PASSWORD"
    )
    sys.exit(1)

SOURCE_REPO = "test-migration"
TARGET_REPO = "dckr"
SOURCE_REGISTRY = "sanich.space:5002"
TARGET_REGISTRY = "sanich.space:8089"

# Образ с Skopeo внутри Docker
SKOPEO_IMAGE = "quay.io/skopeo/stable:latest"


# === Requests-сессия ===
session = requests.Session()
session.auth = (USERNAME, PASSWORD)
session.verify = False
session.headers.update({"Accept": "application/json"})


def get_images_from_repo(repo_name):
    log.info(f"📦 Получаем список образов из Nexus-репозитория '{repo_name}'")
    components_url = f"{NEXUS_URL}/service/rest/v1/components"
    images = []
    continuation_token = None

    while True:
        params = {"repository": repo_name}
        if continuation_token:
            params["continuationToken"] = continuation_token

        resp = session.get(components_url, params=params)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("items", []):
            name = item["name"]
            tag = item["version"]
            if name and tag:
                images.append((name, tag))

        continuation_token = data.get("continuationToken")
        if not continuation_token:
            break

    log.info(f"✅ Найдено {len(images)} образов")
    return images


def skopeo_copy_images(images):
    for name, tag in images:
        src = f"docker://{SOURCE_REGISTRY}/{name}:{tag}"
        dst = f"docker://{TARGET_REGISTRY}/{name}:{tag}"

        log.info(f"🔁 skopeo copy {src} → {dst}")

        cmd = [
            "docker",
            "run",
            "--rm",
            SKOPEO_IMAGE,
            "copy",
            "--src-tls-verify=false",
            "--dest-tls-verify=false",
            "--src-creds",
            f"{USERNAME}:{PASSWORD}",
            "--dest-creds",
            f"{USERNAME}:{PASSWORD}",
            src,
            dst,
        ]

        try:
            subprocess.run(cmd, check=True)
            log.info(f"✅ {name}:{tag} скопирован")
        except subprocess.CalledProcessError as e:
            log.error(f"❌ Ошибка копирования {name}:{tag}: {e}")


def main():
    # parser = argparse.ArgumentParser(description="Миграция Docker-образов через skopeo в контейнере")
    # args = parser.parse_args()

    images = get_images_from_repo(SOURCE_REPO)
    if not images:
        log.warning("Нет образов для миграции.")
        return

    skopeo_copy_images(images)


if __name__ == "__main__":
    main()
