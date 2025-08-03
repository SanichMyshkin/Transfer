import os
import tempfile
import logging
import requests
import subprocess
import urllib3
from dotenv import load_dotenv

# ОБЯЗАТЕЛЬНО РУКАМИ ЛОГИНЕМСЯ ЧЕРЕЗ npm adduser --regisstry=<URL>
# ОБЯЗАТЕЛЬНО РУКАМИ ЛОГИНЕМСЯ ЧЕРЕЗ npm adduser --regisstry=<URL>
# ОБЯЗАТЕЛЬНО РУКАМИ ЛОГИНЕМСЯ ЧЕРЕЗ npm adduser --regisstry=<URL>
# ОБЯЗАТЕЛЬНО РУКАМИ ЛОГИНЕМСЯ ЧЕРЕЗ npm adduser --regisstry=<URL>


# === Загрузка переменных окружения ===
load_dotenv()

# === Логгирование ===
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# === Отключаем SSL-предупреждения ===
urllib3.disable_warnings()

# === Переменные окружения ===
NEXUS_URL = os.getenv("NEXUS_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
SOURCE_REPO = os.getenv("SOURCE_REPO", "source-npm")
TARGET_REPO = os.getenv("TARGET_REPO", "target-npm")

if not all([NEXUS_URL, USERNAME, PASSWORD]):
    log.error("❌ Отсутствуют обязательные переменные окружения (NEXUS_URL, USERNAME, PASSWORD)")
    exit(1)

# === Подготовка URL ===
BASE_URL = NEXUS_URL.rstrip("/")
SOURCE_API = f"{BASE_URL}/service/rest/v1/components"
TARGET_REGISTRY = f"{BASE_URL}/repository/{TARGET_REPO}/"

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
            resp = session.get(SOURCE_API, params=params)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error(f"❌ Ошибка при получении пакетов: {e}")
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
    log.debug(f"🔎 Проверяем {name}@{version} в {TARGET_REPO}")
    continuation_token = None

    while True:
        params = {"repository": TARGET_REPO}
        if continuation_token:
            params["continuationToken"] = continuation_token

        try:
            resp = session.get(SOURCE_API, params=params)
            if resp.status_code == 404:
                log.warning(f"⚠️ Репозиторий {TARGET_REPO} не найден!")
                return False
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error(f"❌ Ошибка запроса к репозиторию: {e}")
            return False

        data = resp.json()
        for item in data.get("items", []):
            if item.get("name") == name and item.get("version") == version:
                log.info(f"✅ {name}@{version} уже загружен")
                return True

        continuation_token = data.get("continuationToken")
        if not continuation_token:
            break

    return False


def download_npm_tarball(name, version, dest_dir):
    log.info(f"⬇️ Скачиваем {name}@{version}")
    try:
        resp = requests.get(f"https://registry.npmjs.org/{name}/{version}")
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Ошибка при запросе npm metadata: {e}")

    tarball_url = resp.json().get("dist", {}).get("tarball")
    if not tarball_url:
        raise ValueError(f"Не найден URL артефакта для {name}@{version}")

    filename = os.path.join(dest_dir, f"{name}-{version}.tgz")
    try:
        with requests.get(tarball_url, stream=True) as r:
            r.raise_for_status()
            with open(filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        raise RuntimeError(f"Ошибка при скачивании {tarball_url}: {e}")

    if not os.path.isfile(filename) or os.path.getsize(filename) == 0:
        raise FileNotFoundError(f"Скачанный файл не найден или пуст: {filename}")

    return filename


def publish_to_nexus(tarball_path):
    if not os.path.isfile(tarball_path):
        log.error(f"❌ Файл не существует: {tarball_path}")
        return

    log.info(f"📦 Публикуем {os.path.basename(tarball_path)}")
    try:
        subprocess.run(
            ["npm", "publish", tarball_path, "--registry", TARGET_REGISTRY],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        log.info("✅ Успешно опубликован")
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode().strip()
        log.error(f"❌ Ошибка публикации: {stderr}")


def migrate_npm_packages():
    packages = get_all_packages(SOURCE_REPO)
    if not packages:
        log.warning("❗ Нет пакетов для миграции")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        for name, version in packages:
            if is_package_uploaded(name, version):
                continue

            try:
                tarball = download_npm_tarball(name, version, tmpdir)
                publish_to_nexus(tarball)
            except Exception as e:
                log.warning(f"⚠️ Ошибка с {name}@{version}: {e}")


def main():
    log.info("🚀 Начинаем миграцию NPM пакетов")
    migrate_npm_packages()
    log.info("✅ Миграция завершена.")


if __name__ == "__main__":
    main()
