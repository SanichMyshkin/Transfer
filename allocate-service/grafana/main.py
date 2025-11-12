import os
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import requests

load_dotenv()
GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER")
GRAFANA_PASS = os.getenv("GRAFANA_PASS")
LOG_FILE = os.getenv("LOG_FILE", "grafana_users_report.log")
OUTPUT_FILE = "grafana_users_report.xlsx"

logger = logging.getLogger("grafana_report")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

if not GRAFANA_URL or not GRAFANA_USER or not GRAFANA_PASS:
    logger.error("Не найдены GRAFANA_URL / GRAFANA_USER / GRAFANA_PASS")
    raise SystemExit(1)

def grafana_get(endpoint, params=None):
    url = f"{GRAFANA_URL.rstrip('/')}/api/{endpoint.lstrip('/')}"
    r = requests.get(url, auth=(GRAFANA_USER, GRAFANA_PASS), params=params, timeout=15)
    if r.status_code == 401:
        logger.error("Ошибка 401: неправильный логин или пароль")
        raise SystemExit(1)
    r.raise_for_status()
    return r.json()

def get_users():
    page = 1
    users = []
    while True:
        data = grafana_get("users/search", params={"perpage": 1000, "page": page})
        if not data.get("users"):
            break
        users.extend(data["users"])
        logger.info(f"Загружено {len(users)} пользователей")
        if len(data["users"]) < 1000:
            break
        page += 1
    return users

def get_user_teams(user_id):
    try:
        r = grafana_get(f"users/{user_id}/teams")
        return [t["name"] for t in r]
    except Exception:
        return []

logger.info("Получаю список пользователей Grafana...")
users = get_users()
logger.info(f"Всего пользователей: {len(users)}")

user_data = []
for u in users:
    uid = u["id"]
    try:
        detail = grafana_get(f"users/{uid}")
    except Exception as e:
        logger.warning(f"Ошибка при получении {uid}: {e}")
        continue
    teams = ", ".join(get_user_teams(uid))
    user_data.append({
        "ID": uid,
        "Логин": detail.get("login", "—"),
        "Email": detail.get("email", "—"),
        "Имя": detail.get("name", "—"),
        "Роль": detail.get("orgRole", "—"),
        "Активен": "Да" if not detail.get("isDisabled") else "Нет",
        "Команды": teams or "—",
        "Дата создания": detail.get("createdAt", "—"),
        "Последний вход": detail.get("lastSeenAt", "—"),
        "Поставщик": ", ".join(detail.get("authLabels", [])) if detail.get("authLabels") else "—",
    })

df = pd.DataFrame(user_data).sort_values(by="Логин")
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="Пользователи", index=False)

logger.info(f"Отчёт сохранён в {OUTPUT_FILE}")
