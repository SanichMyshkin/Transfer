import os
import logging
import pandas as pd
from dotenv import load_dotenv
import requests

load_dotenv()
GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")
LOG_FILE = os.getenv("LOG_FILE", "grafana_users_v1.log")
OUTPUT_FILE = "grafana_users_v1.xlsx"

logger = logging.getLogger("grafana_v1_report")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

if not GRAFANA_URL or not GRAFANA_USER or not GRAFANA_PASS:
    logger.error("❌ Не найдены GRAFANA_URL / GRAFANA_USER / GRAFANA_PASS в .env")
    raise SystemExit(1)

requests.packages.urllib3.disable_warnings()
session = requests.Session()
session.auth = (GRAFANA_USER, GRAFANA_PASS)
session.verify = False  # отключаем SSL проверку

def get_all_users():
    users = []
    page = 1
    while True:
        r = session.get(
            f"{GRAFANA_URL.rstrip('/')}/api/v1/users",
            params={"page": page, "limit": 1000},
            timeout=30
        )
        if r.status_code == 401:
            logger.error("❌ Ошибка 401: неверный логин или недостаточно прав (нужен Server Admin)")
            raise SystemExit(1)
        if r.status_code == 403:
            logger.error("❌ Ошибка 403: доступ к /api/v1/users запрещён. Включи auth.basic и зайди под Server Admin.")
            raise SystemExit(1)
        if r.status_code != 200:
            logger.error(f"Ошибка {r.status_code}: {r.text}")
            break

        data = r.json()
        if not data:
            break
        users.extend(data)
        logger.info(f"Загружено {len(users)} пользователей...")
        if len(data) < 1000:
            break
        page += 1
    return users

logger.info("📥 Получаю пользователей Grafana через /api/v1/users ...")
users = get_all_users()
logger.info(f"📦 Всего пользователей: {len(users)}")

df = pd.DataFrame(users)
df.to_excel(OUTPUT_FILE, index=False)
logger.info(f"📘 Отчёт сохранён в {OUTPUT_FILE}")
