import os
import subprocess
import tempfile
import logging
import requests
import urllib3
import argparse
import sys
from dotenv import load_dotenv


load_dotenv()

# === Логгирование ===
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# === Отключаем SSL предупреждения ===
urllib3.disable_warnings()

# === Чтение конфигурации из переменных окружения ===
NEXUS_URL = os.environ.get("NEXUS_URL")
USERNAME = os.environ.get("USERNAME")
PASSWORD = os.environ.get("PASSWORD")

if not all([NEXUS_URL, USERNAME, PASSWORD]):
    log.error(
        "❌ Не заданы переменные окружения: NEXUS_URL, NEXUS_USERNAME, NEXUS_PASSWORD"
    )
    sys.exit(1)

# === Репозитории ===
SOURCE_REPO = "test-pypi"
TARGET_REPO = "target-pypi"

SOURCE_INDEX_URL = f"{NEXUS_URL}/repository/{SOURCE_REPO}/simple"
TARGET_UPLOAD_URL = f"{NEXUS_URL}/repository/{TARGET_REPO}/"

# === requests с авторизацией ===
session = requests.Session()
session.auth = (USERNAME, PASSWORD)
session.verify = False
session.headers.update({"Accept": "application/json"})


def get_all_packages(repo_name):
    log.info(f"📋 Получаем список пакетов из {repo_name}...")
    url = f"{NEXUS_URL}/service/rest/v1/components"
    continuation_token = None
    packages = []

    while True:
        params = {"repository": repo_name}
        if continuation_token:
            params["continuationToken"] = continuation_token

        resp = session.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        for item in data["items"]:
            name = item["name"]
            version = item["version"]
            packages.append((name, version))

        continuation_token = data.get("continuationToken")
        if not continuation_token:
            break

    log.info(f"🔍 Найдено {len(packages)} пакетов")
    return packages


def is_package_uploaded(name, version):
    log.debug(f"🔎 Проверяем, загружен ли {name}=={version} в {TARGET_REPO}")
    url = f"{NEXUS_URL}/service/rest/v1/components"
    continuation_token = None

    while True:
        params = {"repository": TARGET_REPO}
        if continuation_token:
            params["continuationToken"] = continuation_token

        resp = session.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        for item in data["items"]:
            if item["name"] == name and item["version"] == version:
                log.info(f"✅ {name}=={version} уже загружен, пропускаем")
                return True

        continuation_token = data.get("continuationToken")
        if not continuation_token:
            break

    return False


def pip_download(name, version, download_dir):
    log.info(f"⬇️ pip download: {name}=={version}")

    try:
        subprocess.run(
            [
                "pip",
                "download",
                f"{name}=={version}",
                "--only-binary=:all:",
                "--no-deps",
                "--index-url",
                SOURCE_INDEX_URL,
                "-d",
                download_dir,
            ],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        log.warning(f"⚠️ pip не смог скачать wheel {name}=={version}: {e}")

    try:
        subprocess.run(
            [
                "pip",
                "download",
                f"{name}=={version}",
                "--no-binary=:all:",
                "--no-deps",
                "--index-url",
                SOURCE_INDEX_URL,
                "-d",
                download_dir,
            ],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        log.warning(f"⚠️ pip не смог скачать sdist {name}=={version}: {e}")


def twine_upload(file_path):
    log.info(f"⬆️ Загружаем {os.path.basename(file_path)} через twine")
    try:
        subprocess.run(
            [
                "twine",
                "upload",
                "--repository-url",
                TARGET_UPLOAD_URL,
                "-u",
                USERNAME,
                "-p",
                PASSWORD,
                "--non-interactive",
                file_path,
            ],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        log.error(f"❌ Ошибка загрузки {file_path}: {e}")


def migrate_pypi_packages():
    packages = get_all_packages(SOURCE_REPO)
    if not packages:
        log.warning("❗ Нет пакетов для миграции")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        for name, version in packages:
            if is_package_uploaded(name, version):
                continue

            pip_download(name, version, tmpdir)

        for fname in os.listdir(tmpdir):
            fpath = os.path.join(tmpdir, fname)
            if fname.endswith((".whl", ".tar.gz", ".zip", ".tgz")):
                twine_upload(fpath)
            else:
                log.warning(f"⚠️ Пропущен неподдерживаемый файл: {fname}")


def main():
    parser = argparse.ArgumentParser(
        description="Миграция PyPI пакетов между Nexus-репозиториями через pip и twine"
    )
    args = parser.parse_args()

    migrate_pypi_packages()
    log.info("✅ Миграция завершена.")


if __name__ == "__main__":
    main()
