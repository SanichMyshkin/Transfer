import os
import logging
import pandas as pd
from dotenv import load_dotenv
from grafana_client import GrafanaApi


load_dotenv()
GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")
LOG_FILE = os.getenv("LOG_FILE", "grafana.log")

OUTPUT_FILE = "grafana_report.xlsx"


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
client = GrafanaApi.from_url(
    url=GRAFANA_URL,
    username=GRAFANA_USER,
    password=GRAFANA_PASS
)

# ========================= GET USERS =========================
def get_all_users():
    logger.info("📥 Получаю пользователей...")
    users = client.users.get_all_users()
    logger.info(f"📦 Всего пользователей: {len(users)}")
    return users

# ========================= GET ORGS =========================
def get_all_orgs():
    logger.info("📥 Получаю организации...")
    orgs = client.organizations.get_organizations()
    logger.info(f"🏢 Найдено организаций: {len(orgs)}")
    return orgs

# ========================= GET FOLDERS (текущей организации) =========================
def get_folders():
    logger.info("📂 Получаю папки текущей организации...")
    folders = client.folder.get_all_folders()
    logger.info(f"📁 Найдено папок: {len(folders)}")
    return folders

# ========================= MAIN =========================
users = get_all_users()
orgs = get_all_orgs()
folders = get_folders()

# ========================= SAVE TO ONE EXCEL =========================
df_users = pd.DataFrame(users)
df_orgs = pd.DataFrame(orgs)
df_folders = pd.DataFrame(folders)

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df_users.to_excel(writer, sheet_name="Users", index=False)
    df_orgs.to_excel(writer, sheet_name="Orgs", index=False)
    df_folders.to_excel(writer, sheet_name="Folders", index=False)

logger.info(f"📘 Отчёт сохранён в {OUTPUT_FILE}")
logger.info("✅ Готово!")
