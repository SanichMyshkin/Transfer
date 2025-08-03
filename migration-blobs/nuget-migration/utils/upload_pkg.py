import os
import tempfile
import logging
import requests
import subprocess
from dotenv import load_dotenv

load_dotenv()

"""
Скачать dotnet
wget https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt update
sudo apt install -y dotnet-sdk-8.0
"""


"""
Добавить репо
dotnet nuget add source \  --name nexus \
  --username usr \
  --password paswrd \
  --store-password-in-clear-text \
  https://nexus.sanich.space/repository/source-nuget/
"""

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

NEXUS_URL = os.getenv("NEXUS_URL")
NUGET_REPO = os.getenv("NUGET_REPO", "source-nuget")
NUGET_API_KEY = os.getenv("NUGET_API_KEY", "dummy-key")
REGISTRY_URL = f"{NEXUS_URL}/repository/{NUGET_REPO}/"

nuget_packages = [
    ("Newtonsoft.Json", "13.0.3"),
    ("NUnit", "3.13.3"),
    ("FluentValidation", "11.7.1"),
    ("Polly", "7.2.3"),
    ("AutoFixture", "4.18.0"),
    ("Bogus", "35.0.1"),
    ("Humanizer", "2.14.1"),
]


def package_exists(name, version):
    url = f"https://api.nuget.org/v3-flatcontainer/{name.lower()}/{version}/{name.lower()}.{version}.nupkg"
    r = requests.head(url)
    return r.status_code == 200


def download_nuget_package(name, version, dest_dir):
    if not package_exists(name, version):
        raise FileNotFoundError(f"❌ {name} {version} не существует на nuget.org")

    log.info(f"⬇️ Скачиваем {name} {version}")
    package_url = f"https://api.nuget.org/v3-flatcontainer/{name.lower()}/{version}/{name.lower()}.{version}.nupkg"
    file_path = os.path.join(dest_dir, f"{name}.{version}.nupkg")

    try:
        with requests.get(package_url, stream=True) as r:
            r.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        raise RuntimeError(f"Ошибка при скачивании {package_url}: {e}")

    if not os.path.isfile(file_path) or os.path.getsize(file_path) == 0:
        raise FileNotFoundError(f"Файл пустой или не создан: {file_path}")

    return file_path


def publish_to_nexus(nupkg_path):
    log.info(f"📦 Публикуем {os.path.basename(nupkg_path)} в Nexus")
    try:
        result = subprocess.run(
            [
                "dotnet",
                "nuget",
                "push",
                nupkg_path,
                "--source",
                REGISTRY_URL,
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
        stderr = e.stderr.decode(errors="ignore")
        stdout = e.stdout.decode(errors="ignore")
        log.error(
            f"❌ Ошибка публикации:\nSTDERR:\n{stderr.strip()}\nSTDOUT:\n{stdout.strip()}"
        )


def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        for name, version in nuget_packages:
            try:
                nupkg = download_nuget_package(name, version, tmpdir)
                publish_to_nexus(nupkg)
            except Exception as e:
                log.warning(f"⚠️ Ошибка с {name} {version}: {e}")


if __name__ == "__main__":
    main()
