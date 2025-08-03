# просто pull'ит и push'ит образы через docker, без skopeo

import argparse
import logging
import subprocess
import requests
import docker
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

SOURCE_REPO = "docker-file-single-1"
TARGET_REPO = "dckr"
SOURCE_REGISTRY = "sanich.space:5000"
TARGET_REGISTRY = "sanich.space:8088"


# === Requests с auth ===
session = requests.Session()
session.auth = (USERNAME, PASSWORD)
session.verify = False  # Отключаем проверку SSL (если самоподписанный)
session.headers.update({"Accept": "application/json"})


def docker_login(registry, username, password):
    log.info(f"Выполняем docker login в {registry}")
    try:
        result = subprocess.run(
            ["docker", "login", registry, "-u", username, "--password-stdin"],
            input=password.encode(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        log.info(result.stdout.decode().strip())
    except subprocess.CalledProcessError as e:
        log.error(f"Ошибка docker login в {registry}: {e.stderr.decode().strip()}")
        raise


def get_images_from_repo(repo_name):
    log.info(f"Получаем образы из Nexus-репозитория: {repo_name}")
    images = []
    continuation_token = None
    base_url = f"{NEXUS_URL}/service/rest/v1/components"

    while True:
        params = {"repository": repo_name}
        if continuation_token:
            params["continuationToken"] = continuation_token

        response = session.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        for item in data["items"]:
            name = item["name"]
            tag = item["version"]
            if tag:
                images.append((name, tag))

        continuation_token = data.get("continuationToken")
        if not continuation_token:
            break

    log.info(f"Найдено {len(images)} образов")
    return images


def pull_tag_push(images):
    docker_client = docker.from_env()

    for name, tag in images:
        source_image = f"{SOURCE_REGISTRY}/{name}:{tag}"
        target_image = f"{TARGET_REGISTRY}/{name}:{tag}"

        log.info(f"▶️ Pull: {source_image}")
        docker_client.images.pull(source_image)

        log.info(f"🔁 Tag: {source_image} -> {target_image}")
        docker_client.images.get(source_image).tag(target_image)

        log.info(f"📤 Push: {target_image}")
        for line in docker_client.images.push(target_image, stream=True, decode=True):
            if "status" in line:
                log.debug(line["status"])

        log.info("🗑 Удаление локальных образов")
        try:
            log.info(f"🗑 Удаляем локальный образ: {source_image}")
            docker_client.images.remove(source_image, force=True)
        except Exception as e:
            log.warning(f"Не удалось удалить {source_image}: {e}")
        try:
            log.info(f"🗑 Удаляем локальный образ: {target_image}")
            docker_client.images.remove(target_image, force=True)
        except Exception as e:
            log.warning(f"Не удалось удалить {target_image}: {e}")


def delete_repo_if_unused(repo_name):
    log.info(f"🧹 Удаление репозитория {repo_name} и связанного blob store")
    # 1. Проверим, какие еще репозитории используют тот же blob store
    repos = session.get(f"{NEXUS_URL}/service/rest/v1/repositories").json()
    target_repo = next((r for r in repos if r["name"] == repo_name), None)

    if not target_repo:
        log.warning(f"Репозиторий {repo_name} не найден")
        return

    blob_store = target_repo.get("storage", {}).get("blobStoreName")
    users_of_blob = [
        r["name"]
        for r in repos
        if r.get("storage", {}).get("blobStoreName") == blob_store
    ]

    if len(users_of_blob) > 1:
        log.warning(
            f"Блоб {blob_store} также используется в: {users_of_blob}. Не удаляем."
        )
    else:
        # Удаляем репозиторий
        log.info(f"Удаляем репозиторий {repo_name}")
        del_resp = session.delete(
            f"{NEXUS_URL}/service/rest/v1/repositories/{repo_name}"
        )
        if del_resp.status_code == 204:
            log.info(f"✅ Репозиторий {repo_name} удалён")
        else:
            log.error(f"Ошибка удаления: {del_resp.status_code} {del_resp.text}")
        # (Опционально) удалить blob — только через admin API или вручную


def main():
    parser = argparse.ArgumentParser(
        description="Миграция Docker-образов между репозиториями Nexus"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Удалить исходный репозиторий и блоб (если не используется)",
    )
    args = parser.parse_args()

    # Docker логины
    docker_login(SOURCE_REGISTRY, USERNAME, PASSWORD)
    docker_login(TARGET_REGISTRY, USERNAME, PASSWORD)

    # Получение образов
    images = get_images_from_repo(SOURCE_REPO)
    if not images:
        log.warning("Нет образов для миграции.")
        return

    # Перенос
    pull_tag_push(images)

    # Очистка
    if args.cleanup:
        delete_repo_if_unused(SOURCE_REPO)


if __name__ == "__main__":
    main()
