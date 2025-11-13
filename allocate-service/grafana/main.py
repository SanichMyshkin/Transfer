import os
import logging
import pandas as pd
from dotenv import load_dotenv
import requests

load_dotenv()
GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")
LOG_FILE = os.getenv("LOG_FILE", "grafana_report.log")
OUTPUT_FILE = "grafana_report.xlsx"


logger = logging.getLogger("grafana_report")
logger.setLevel(logging.INFO)

fmt = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
)

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
            f"{GRAFANA_URL.rstrip('/')}/api/users",
            params={"page": page, "limit": 1000},
            timeout=30,
        )

        if r.status_code == 401:
            logger.error(
                "❌ 401: неверный логин или недостаточно прав (нужен Server Admin)"
            )
            raise SystemExit(1)

        if r.status_code == 403:
            logger.error(
                "❌ 403: доступ к /api/users запрещён. Включи auth.basic и зайди под Server Admin."
            )
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



def get_all_orgs():
    r = session.get(f"{GRAFANA_URL.rstrip('/')}/api/orgs", timeout=30)

    if r.status_code == 401:
        logger.error(
            "❌ 401: неверный логин или недостаточно прав для получения организаций"
        )
        raise SystemExit(1)

    if r.status_code == 403:
        logger.error("❌ 403: доступ к /api/orgs запрещён. Нужны права Server Admin.")
        raise SystemExit(1)

    if r.status_code != 200:
        logger.error(f"Ошибка {r.status_code}: {r.text}")
        raise SystemExit(1)

    orgs = r.json()
    logger.info(f"🏢 Найдено организаций: {len(orgs)}")
    return orgs



logger.info("📥 Получаю пользователей Grafana через /api/users ...")
users = get_all_users()
logger.info(f"📦 Всего пользователей: {len(users)}")

logger.info("📥 Получаю организации Grafana ...")
orgs = get_all_orgs()

df_users = pd.DataFrame(users)
df_orgs = pd.DataFrame(orgs)

# ========================= SAVE TO ONE EXCEL WITH SHEETS =========================
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df_users.to_excel(writer, sheet_name="Users", index=False)
    df_orgs.to_excel(writer, sheet_name="Orgs", index=False)

logger.info(f"📘 Отчёт сохранён в {OUTPUT_FILE} (листы: Users, Orgs)")
logger.info("✅ Готово!")
