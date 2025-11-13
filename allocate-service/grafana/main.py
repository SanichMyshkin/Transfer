import os
import logging
import pandas as pd
from dotenv import load_dotenv
from grafana_client import GrafanaApi

# ========================= LOAD ENV =========================
load_dotenv()
GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")
LOG_FILE = os.getenv("LOG_FILE", "grafana_report.log")
OUTPUT_FILE = "grafana_report.xlsx"

# ========================= LOGGING =========================
logger = logging.getLogger("grafana_report")
logger.setLevel(logging.INFO)

fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")

fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

# ========================= CHECK ENV =========================
if not GRAFANA_URL or not GRAFANA_USER or not GRAFANA_PASS:
    logger.error("❌ Не найдены GRAFANA_URL / GRAFANA_USER / GRAFANA_PASS в .env")
    raise SystemExit(1)

# ========================= GRAFANA CLIENT =========================
client = GrafanaApi(
    auth=(GRAFANA_USER, GRAFANA_PASS),
    host=GRAFANA_URL,
)

# ========================= GET USERS =========================
def get_all_users():
    logger.info("📥 Получаю пользователей Grafana через grafana-client ...")
    try:
        users = client.users.get_all_users()
        logger.info(f"📦 Всего пользователей: {len(users)}")
        return users
    except Exception as e:
        logger.error(f"Ошибка при получении пользователей: {e}")
        raise SystemExit(1)

# ========================= GET ORGS =========================
def get_all_orgs():
    logger.info("📥 Получаю организации Grafana ...")
    try:
        orgs = client.organizations.get_organizations()
        logger.info(f"🏢 Найдено организаций: {len(orgs)}")
        return orgs
    except Exception as e:
        logger.error(f"Ошибка при получении организаций: {e}")
        raise SystemExit(1)

# ========================= MAIN =========================
users = get_all_users()
orgs = get_all_orgs()

# ========================= SAVE TO EXCEL =========================
df_users = pd.DataFrame(users)
df_orgs = pd.DataFrame(orgs)

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df_users.to_excel(writer, sheet_name="Users", index=False)
    df_orgs.to_excel(writer, sheet_name="Orgs", index=False)

logger.info(f"📘 Отчёт сохранён в {OUTPUT_FILE} (листы: Users, Orgs)")
logger.info("✅ Готово!")
