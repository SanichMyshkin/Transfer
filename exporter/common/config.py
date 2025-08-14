import os
from dotenv import load_dotenv

load_dotenv()

# 🔗 Nexus настройки
NEXUS_API_URL = os.getenv("NEXUS_API_URL")
NEXUS_USERNAME = os.getenv("NEXUS_USERNAME")
NEXUS_PASSWORD = os.getenv("NEXUS_PASSWORD")

# 🔐 GitLab настройки
GITLAB_URL = os.getenv("GITLAB_URL", "https://gitlab.fc.uralsibbank.ru")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
GITLAB_BRANCH = os.getenv("GITLAB_BRANCH", "main")  # ветка по умолчанию

# 📊 Прочие настройки
DATABASE_URL = os.getenv("DATABASE_URL")
REPO_METRICS_INTERVAL = int(os.getenv("REPO_METRICS_INTERVAL", "1800"))
LAUNCH_INTERVAL = int(os.getenv("LAUNCH_INTERVAL", "300"))


def get_auth():
    return (NEXUS_USERNAME, NEXUS_PASSWORD)