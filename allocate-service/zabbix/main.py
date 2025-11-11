import os
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

# === ЗАГРУЗКА .env ===
load_dotenv()
ZABBIX_URL = os.getenv("ZABBIX_URL")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN")
LOG_FILE = os.getenv("LOG_FILE", "zabbix_utils_report.log")
OUTPUT_FILE = "zabbix_users_report.xlsx"

# === НАСТРОЙКА ЛОГГЕРА ===
logger = logging.getLogger("zabbix_utils_report")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")

fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

# === ПРОВЕРКА НАСТРОЕК ===
if not ZABBIX_URL or not ZABBIX_TOKEN:
    logger.error("❌ Не найден URL или TOKEN. Проверь .env файл.")
    raise SystemExit(1)

# === ПОДКЛЮЧЕНИЕ К ZABBIX ===
logger.info("🔗 Подключаюсь к Zabbix через python-zabbix-utils...")
api = ZabbixAPI(url=ZABBIX_URL)
api.login(token=ZABBIX_TOKEN)
logger.info("✅ Подключение успешно!")

# === ЗАПРОС ПОЛЬЗОВАТЕЛЕЙ ===
logger.info("📥 Загружаю список пользователей...")
users = api.user.get(
    output=['userid', 'alias', 'username', 'name', 'surname', 'type', 'autologin', 'lang'],
    selectUsrgrps=['name'],
    selectRole=['name'],
    selectSessions=['lastaccess'],
    selectMedias=['sendto']
)

logger.info(f"📦 Получено пользователей: {len(users)}")

# === ОБРАБОТКА ДАННЫХ ===
roles_map = {0: "User", 1: "Admin", 2: "Super Admin"}
data = []

for u in users:
    # Получаем логин (в 7.x может быть username вместо alias)
    login = u.get("alias") or u.get("username") or "—"

    # --- обработка email / медиа ---
    medias = []
    for m in u.get("medias", []):
        s = m.get("sendto")
        if isinstance(s, list):
            medias.extend(s)
        elif isinstance(s, str):
            medias.append(s)
    email = ", ".join(medias) if medias else "—"

    # --- остальные поля ---
    groups = ", ".join(g["name"] for g in u.get("usrgrps", []))
    role = u.get("role", {}).get("name", roles_map.get(int(u.get("type", 0)), "N/A"))

    last_ts = u.get("sessions", [{}])[0].get("lastaccess")
    if last_ts:
        last_login = datetime.utcfromtimestamp(int(last_ts)).strftime("%Y-%m-%d %H:%M:%S")
    else:
        last_login = "—"

    autologin = "Да" if u.get("autologin") == "1" else "Нет"

    data.append({
        "ID": u.get("userid", "—"),
        "Логин": login,
        "Имя": f"{u.get('name','')} {u.get('surname','')}".strip() or "—",
        "Email": email,
        "Группы": groups or "—",
        "Роль": role,
        "Последний вход": last_login,
        "Автовход": autologin,
        "Язык интерфейса": u.get("lang", "—")
    })

# === СОХРАНЕНИЕ В EXCEL ===
logger.info("💾 Сохраняю отчёт...")
df = pd.DataFrame(data)
df.sort_values(by="Логин", inplace=True)
df.to_excel(OUTPUT_FILE, index=False)
logger.info(f"📊 Отчёт сохранён в {OUTPUT_FILE}")

# === ЗАВЕРШЕНИЕ ===
api.logout()
logger.info("🔒 Сессия закрыта. Готово ✅")
