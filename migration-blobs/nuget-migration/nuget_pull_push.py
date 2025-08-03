import os
import tempfile
import logging
import requests
import subprocess
import urllib3
from dotenv import load_dotenv

"""
dotnet nuget add source \
  --name nexus \
  --username <username> \
  --password <password> \
  --store-password-in-clear-text \
  https://<nexus-host>/repository/<repo-name>/
"""


# === Загрузка .env ===
load_dotenv()

# === Логгирование ===
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

urllib3.disable_warnings()

# === Переменные окружения ===
NEXUS_URL = os.getenv("NEXUS_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
SOURCE_REPO = os.getenv("SOURCE_REPO", "source-nuget")
TARGET_REPO = os.getenv("TARGET_REPO", "target-nuget")
NUGET_API_KEY = os.getenv("NUGET_API_KEY", "dummy-key")  # Nexus может требовать токен

if not all([NEXUS_URL, USERNAME, PASSWORD]):
    log.error(
        "❌ Не заданы обязательные переменные окружения (NEXUS_URL, USERNAME, PASSWORD)"
    )
    exit(1)

# === URL-ы ===
BASE_URL = NEXUS_URL.rstrip("/")
NEXUS_COMPONENTS_API = f"{BASE_URL}/service/rest/v1/components"
TARGET_PUSH_URL = f"{BASE_URL}/repository/{TARGET_REPO}/"

session = requests.Session()
session.auth = (USERNAME, PASSWORD)
session.verify = False
session.headers.update({"Accept": "application/json"})


def get_all_packages(repo_name):
    log.info(f"📋 Получаем список пакетов из {repo_name}...")
    continuation_token = None
    packages = []

    while True:
        params = {"repository": repo_name}
        if continuation_token:
            params["continuationToken"] = continuation_token

        try:
            resp = session.get(NEXUS_COMPONENTS_API, params=params)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error(f"❌ Ошибка запроса к Nexus: {e}")
            break

        data = resp.json()
        for item in data.get("items", []):
            name = item.get("name")
            version = item.get("version")
            if name and version:
                packages.append((name, version))

        continuation_token = data.get("continuationToken")
        if not continuation_token:
            break

    log.info(f"🔍 Найдено {len(packages)} пакетов")
    return packages


def is_package_uploaded(name, version):
    log.debug(f"🔎 Проверяем {name} {version} в {TARGET_REPO}")
    continuation_token = None

    while True:
        params = {"repository": TARGET_REPO}
        if continuation_token:
            params["continuationToken"] = continuation_token

        try:
            resp = session.get(NEXUS_COMPONENTS_API, params=params)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error(f"❌ Ошибка проверки наличия пакета: {e}")
            return False

        for item in resp.json().get("items", []):
            if item.get("name") == name and item.get("version") == version:
                log.info(f"✅ {name} {version} уже загружен")
                return True

        continuation_token = resp.json().get("continuationToken")
        if not continuation_token:
            break

    return False


def download_nuget_package(name, version, dest_dir):
    log.info(f"⬇️ Скачиваем {name} {version}")
    url = f"https://api.nuget.org/v3-flatcontainer/{name.lower()}/{version}/{name.lower()}.{version}.nupkg"
    file_path = os.path.join(dest_dir, f"{name}.{version}.nupkg")

    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
    except Exception as e:
        raise RuntimeError(f"Ошибка скачивания {url}: {e}")

    if not os.path.isfile(file_path) or os.path.getsize(file_path) == 0:
        raise FileNotFoundError(f"Файл не найден или пустой: {file_path}")

    return file_path


def publish_to_nexus(nupkg_path):
    if not os.path.isfile(nupkg_path):
        log.error(f"❌ Файл не существует: {nupkg_path}")
        return

    log.info(f"📦 Публикуем {os.path.basename(nupkg_path)}")
    try:
        subprocess.run(
            [
                "dotnet",
                "nuget",
                "push",
                nupkg_path,
                "--source",
                TARGET_PUSH_URL,
                "--api-key",
                NUGET_API_KEY,
                "--skip-duplicate",
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        log.info("✅ Успешно опубликован")
    except subprocess.CalledProcessError as e:
        log.error(f"❌ Ошибка публикации: {e.stderr.decode().strip()}")


def migrate_nuget_packages():
    packages = get_all_packages(SOURCE_REPO)
    if not packages:
        log.warning("❗ Нет пакетов для миграции")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        for name, version in packages:
            if is_package_uploaded(name, version):
                continue

            try:
                nupkg = download_nuget_package(name, version, tmpdir)
                publish_to_nexus(nupkg)
            except Exception as e:
                log.warning(f"⚠️ Ошибка {name} {version}: {e}")


def main():
    log.info("🚀 Начинаем миграцию NuGet пакетов")
    migrate_nuget_packages()
    log.info("✅ Миграция завершена")


if __name__ == "__main__":
    main()
