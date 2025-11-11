import os
import logging
from datetime import datetime
import pandas as pd
from zabbix_utils import ZabbixAPI
from dotenv import load_dotenv

# === Загружаем переменные окружения ===
load_dotenv()
ZABBIX_URL = os.getenv("ZABBIX_URL")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN")
LOG_FILE = os.getenv("LOG_FILE", "zabbix_utils_report.log")
OUTPUT_FILE = "zabbix_users_report.xlsx"

# === Настройка логирования ===
logger = logging.getLogger("zabbix_utils_report")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
)

fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

if not ZABBIX_URL or not ZABBIX_TOKEN:
    logger.error("❌ Не найден URL или TOKEN. Проверь .env файл.")
    raise SystemExit(1)

# === Подключение к Zabbix через библиотеку ===
logger.info("🔗 Подключаюсь к Zabbix через python-zabbix-utils...")
api = ZabbixAPI(url=ZABBIX_URL)
api.login(token=ZABBIX_TOKEN)
logger.info("✅ Подключение успешно!")

# === Запрос пользователей ===
logger.info("📥 Загружаю список пользователей...")
users = api.user.get(
    output=["userid", "alias", "name", "surname", "type", "autologin", "lang"],
    selectUsrgrps=["name"],
    selectRole=["name"],
    selectSessions=["lastaccess"],
    selectMedias=["sendto"],
)
logger.info(f"📦 Получено пользователей: {len(users)}")

# === Обработка ===
roles_map = {0: "User", 1: "Admin", 2: "Super Admin"}
data = []

for u in users:
    email = ", ".join(m["sendto"] for m in u.get("medias", []) if "sendto" in m)
    groups = ", ".join(g["name"] for g in u.get("usrgrps", []))
    role = u.get("role", {}).get("name", roles_map.get(int(u.get("type", 0)), "N/A"))

    last_ts = u.get("sessions", [{}])[0].get("lastaccess")
    last_login = (
        datetime.utcfromtimestamp(int(last_ts)).strftime("%Y-%m-%d %H:%M:%S")
        if last_ts
        else "—"
    )
    autologin = "Да" if u.get("autologin") == "1" else "Нет"

    data.append(
        {
            "ID": u["userid"],
            "Логин": u["alias"],
            "Имя": f"{u.get('name', '')} {u.get('surname', '')}".strip(),
            "Email": email or "—",
            "Группы": groups,
            "Роль": role,
            "Последний вход": last_login,
            "Автовход": autologin,
            "Язык интерфейса": u.get("lang", "—"),
        }
    )

# === Сохранение ===
df = pd.DataFrame(data)
df.sort_values(by="Логин", inplace=True)
df.to_excel(OUTPUT_FILE, index=False)
logger.info(f"📊 Отчёт сохранён в {OUTPUT_FILE}")

api.logout()
logger.info("🔒 Сессия закрыта. Готово ✅")
