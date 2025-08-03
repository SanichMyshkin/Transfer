import os
import tempfile
import logging
import requests
import subprocess
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

NEXUS_URL = os.getenv("NEXUS_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
PYPI_REPO = os.getenv("PYPI_REPO", "source-pypi")
REGISTRY_URL = f"{NEXUS_URL}/repository/{PYPI_REPO}/"

pypi_packages = [
    ("httpx", "0.27.0"),
    ("SQLAlchemy", "2.0.30"),
    ("Jinja2", "3.1.3"),
    ("requests", "2.31.0"),
    ("pydantic", "1.10.9"),
    ("black", "23.1.0"),
    ("pytest", "7.4.0"),
]

def package_exists_in_nexus(package_name, version):
    simple_url = f"{REGISTRY_URL.rstrip('/')}/simple/{package_name.lower()}/"
    try:
        r = requests.get(simple_url, timeout=5)
        if r.status_code != 200:
            return False
        return f"{package_name}-{version}" in r.text
    except requests.RequestException as e:
        log.warning(f"⚠️ Не удалось проверить наличие {package_name} в Nexus: {e}")
        return False

def download_pypi_package(name, version, dest_dir):
    log.info(f"⬇️ Скачиваем {name}=={version}")
    try:
        subprocess.run(
            [
                "pip",
                "download",
                f"{name}=={version}",
                "-d",
                dest_dir,
                "--no-deps",
                "--quiet",
            ],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Ошибка скачивания {name}=={version}: {e}")

    for filename in os.listdir(dest_dir):
        if filename.lower().startswith(name.lower()) and version in filename:
            return os.path.join(dest_dir, filename)

    raise FileNotFoundError(f"Пакет {name}=={version} не найден после скачивания")

def publish_to_nexus(package_path):
    log.info(f"📦 Публикуем {os.path.basename(package_path)} в Nexus")
    try:
        result = subprocess.run(
            [
                "twine",
                "upload",
                "--repository-url",
                REGISTRY_URL,
                "-u",
                USERNAME,
                "-p",
                PASSWORD,
                package_path,
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        log.info("✅ Успешно опубликован")
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode(errors="ignore")
        stdout = e.stdout.decode(errors="ignore")
        log.error(f"❌ Ошибка публикации:\nSTDERR:\n{stderr.strip()}\nSTDOUT:\n{stdout.strip()}")

def main():
    if not USERNAME or not PASSWORD:
        log.error("❌ Не указаны переменные USERNAME и PASSWORD. Укажите их в .env или переменных окружения.")
        return

    if not NEXUS_URL:
        log.error("❌ Не указана переменная NEXUS_URL.")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        for name, version in pypi_packages:
            if package_exists_in_nexus(name, version):
                log.info(f"✅ {name}=={version} уже есть в Nexus — пропускаем")
                continue
            try:
                package_path = download_pypi_package(name, version, tmpdir)
                publish_to_nexus(package_path)
            except Exception as e:
                log.warning(f"⚠️ Ошибка с {name}=={version}: {e}")

if __name__ == "__main__":
    main()
