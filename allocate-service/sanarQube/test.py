import os
import requests
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

SONAR_URL = os.getenv("SONAR_URL")
TOKEN = os.getenv("SONAR_TOKEN")

session = requests.Session()
session.auth = (TOKEN, "")

def download_background_tasks_html(project_key, filename="page.html"):
    url = f"{SONAR_URL}/project/background_tasks"
    params = {"id": project_key}

    print(f"GET {url} params={params}")

    r = session.get(url, params=params, verify=False)
    r.raise_for_status()

    with open(filename, "w", encoding="utf-8") as f:
        f.write(r.text)

    print(f"HTML сохранён в {filename}")


if __name__ == "__main__":
    # ВСТАВЬ КЛЮЧ ПРОЕКТА СЮДА
    download_background_tasks_html("YOUR_PROJECT_KEY_HERE")
