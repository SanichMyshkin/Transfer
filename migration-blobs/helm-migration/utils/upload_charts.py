import os
import subprocess
import shutil
import logging
from pathlib import Path
from dotenv import load_dotenv

# === Настройка логов ===
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# === Загрузка переменных окружения ===
load_dotenv()

HELM_REPO_URL = os.getenv("NEXUS_URL")
HELM_REPO_USERNAME = os.getenv("USERNAME")
HELM_REPO_PASSWORD = os.getenv("HPASSWORD")
HELM_REPO_NAME = os.getenv("HELM_REPO_NAME", "source-helm")
CHARTS_DIR = Path("./tmp")
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

# === Примерный список чартов (~50) ===
chart_list = [
    ("bitnami", "nginx"),
    ("bitnami", "mysql"),
    ("bitnami", "postgresql"),
    ("bitnami", "redis"),
    ("bitnami", "mariadb"),
    ("bitnami", "mongodb"),
    ("bitnami", "rabbitmq"),
    ("bitnami", "apache"),
    ("bitnami", "elasticsearch"),
    ("bitnami", "grafana"),
    ("bitnami", "kibana"),
    ("bitnami", "jenkins"),
    ("bitnami", "drupal"),
    ("bitnami", "magento"),
    ("bitnami", "joomla"),
    ("bitnami", "tomcat"),
    ("bitnami", "wordpress"),
    ("bitnami", "phpbb"),
    ("bitnami", "prestashop"),
    ("bitnami", "suitecrm"),
    ("bitnami", "owncloud"),
    ("bitnami", "odoo"),
    ("bitnami", "gitlab"),
    ("bitnami", "keycloak"),
    ("bitnami", "mediawiki"),
    ("bitnami", "zookeeper"),
    ("bitnami", "influxdb"),
    ("bitnami", "kafka"),
    ("bitnami", "etcd"),
    ("bitnami", "airflow"),
    ("bitnami", "solr"),
    ("bitnami", "harbor"),
    ("bitnami", "concourse"),
    ("bitnami", "mattermost"),
    ("bitnami", "wildfly"),
    ("bitnami", "redmine"),
    ("bitnami", "parse"),
    ("bitnami", "minio"),
    ("bitnami", "nats"),
    ("bitnami", "haproxy"),
    ("bitnami", "fluentd"),
    ("bitnami", "metallb"),
    ("bitnami", "opencart"),
    ("bitnami", "osclass"),
    ("bitnami", "apache-airflow"),
    ("bitnami", "superset"),
    ("bitnami", "memcached"),
    ("bitnami", "metrics-server"),
    ("bitnami", "contour"),
]

# === Уникальные репозитории ===
unique_repos = {repo for repo, _ in chart_list}
REPO_URLS = {
    "bitnami": "https://charts.bitnami.com/bitnami",
    "minio": "https://helm.min.io/",
}

def add_repos():
    for repo in unique_repos:
        repo_url = REPO_URLS.get(repo)
        if not repo_url:
            log.warning(f"⚠️ Нет URL для репозитория '{repo}', пропущено")
            continue

        subprocess.run(
            ["helm", "repo", "add", repo, repo_url],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    subprocess.run(["helm", "repo", "update"], check=True)
    log.info("✅ Обновлены все helm-репозитории")

def download_charts():
    for repo, chart in chart_list:
        log.info(f"⬇️ Скачиваем чарт {repo}/{chart}")
        try:
            subprocess.run(
                ["helm", "pull", f"{repo}/{chart}", "--destination", str(CHARTS_DIR)],
                check=True
            )
        except subprocess.CalledProcessError as e:
            log.warning(f"⚠️ Не удалось скачать {repo}/{chart}: {e}")

def upload_charts():
    for chart_file in CHARTS_DIR.glob("*.tgz"):
        log.info(f"⬆️ Загружаем {chart_file.name}")
        try:
            subprocess.run(
                [
                    "curl",
                    "-u", f"{HELM_REPO_USERNAME}:{HELM_REPO_PASSWORD}",
                    "--insecure",  # Убрать, если есть валидный TLS
                    "--upload-file", str(chart_file),
                    f"{HELM_REPO_URL}/{chart_file.name}"
                ],
                check=True
            )
        except subprocess.CalledProcessError as e:
            log.error(f"❌ Ошибка загрузки {chart_file.name}: {e}")

if __name__ == "__main__":
    add_repos()
    download_charts()
    upload_charts()
    shutil.rmtree(CHARTS_DIR)
    log.info("✅ Загрузка чартов завершена. Временные файлы удалены.")
