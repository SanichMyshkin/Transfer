import os
import tempfile
import logging
import requests
import subprocess
from dotenv import load_dotenv

# ОБЯЗАТЕЛЬНО РУКАМИ ЛОГИНЕМСЯ ЧЕРЕЗ npm adduser --regisstry=<URL>

load_dotenv()

# === Логгирование ===
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# === Переменные окружения ===
NEXUS_URL = os.getenv("NEXUS_URL")  # Без https:// на конце
NPM_REPO = os.getenv("NPM_REPO", "source-npm")
REGISTRY_URL = f"{NEXUS_URL}/repository/{NPM_REPO}"

# === Список пакетов ===
npm_packages = [
    ("lodash", "4.17.21"),
    ("axios", "1.6.8"),
    ("chalk", "5.3.0"),
    ("commander", "11.1.0"),
    ("express", "4.18.2"),
    ("moment", "2.29.4"),
    ("dotenv", "16.3.1"),
    ("debug", "4.3.4"),
    ("uuid", "9.0.1"),
    ("react", "18.2.0"),
]


def download_npm_tarball(name, version, dest_dir):
    log.info(f"⬇️ Скачиваем {name}@{version}")
    url = f"https://registry.npmjs.org/{name}/{version}"
    resp = requests.get(url)
    resp.raise_for_status()
    tarball_url = resp.json()["dist"]["tarball"]

    filename = os.path.join(dest_dir, f"{name}-{version}.tgz")
    with requests.get(tarball_url, stream=True) as r:
        r.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return filename


def publish_to_nexus(tarball_path):
    log.info(f"📦 Публикуем {os.path.basename(tarball_path)} в Nexus")
    try:
        subprocess.run(
            [
                "npm", "publish", tarball_path,
                "--registry", REGISTRY_URL
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        log.info("✅ Успешно опубликован")
    except subprocess.CalledProcessError as e:
        log.error(f"❌ Ошибка публикации: {e.stderr.decode()}")


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        for name, version in npm_packages:
            try:
                tarball = download_npm_tarball(name, version, tmpdir)
                publish_to_nexus(tarball)
            except Exception as e:
                log.warning(f"⚠️ Ошибка с {name}@{version}: {e}")


if __name__ == "__main__":
    main()
