import os
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

# === ЗАГРУЗКА НАСТРОЕК ===
load_dotenv()
ZABBIX_URL = os.getenv("ZABBIX_URL")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN")
LOG_FILE = os.getenv("LOG_FILE", "zabbix_userdata_report.log")
OUTPUT_FILE = "zabbix_users_full_report.xlsx"

# === ЛОГИ ===
logger = logging.getLogger("zabbix_userdata_report")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")

fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

if not ZABBIX_URL or not ZABBIX_TOKEN:
    logger.error("❌ Не найден URL или TOKEN. Проверь .env")
    raise SystemExit(1)

# === ПОДКЛЮЧЕНИЕ ===
logger.info("🔗 Подключаюсь к Zabbix через python-zabbix-utils...")
api = ZabbixAPI(url=ZABBIX_URL)
api.login(token=ZABBIX_TOKEN)
logger.info("✅ Подключение успешно!")

# === USERS ===
logger.info("📥 Получаю список пользователей...")
users = api.user.get(
    output=[
        "userid", "alias", "username", "name", "surname", "type",
        "autologin", "autologout", "lang", "refresh", "theme",
        "attempt_ip", "attempt_clock", "timezone", "roleid"
    ],
    selectUsrgrps=["name"],
    selectRole=["name"],
    selectSessions=["lastaccess"],
    selectMedias=["sendto"]
)
logger.info(f"📦 Пользователей: {len(users)}")

roles_map = {0: "User", 1: "Admin", 2: "Super Admin"}
user_data = []

for u in users:
    login = u.get("alias") or u.get("username") or "—"

    # --- обработка email ---
    medias = []
    for m in u.get("medias", []):
        s = m.get("sendto")
        if isinstance(s, list):
            medias.extend(s)
        elif isinstance(s, str):
            medias.append(s)
    email = ", ".join(medias) if medias else "—"

    # --- группы и роль ---
    groups = ", ".join(g["name"] for g in u.get("usrgrps", []))
    role_name = u.get("role", {}).get("name", roles_map.get(int(u.get("type", 0)), "N/A"))
    role_id = u.get("roleid", "—")

    # --- логин / IP / время ---
    last_ts = u.get("sessions", [{}])[0].get("lastaccess")
    last_login = (
        datetime.utcfromtimestamp(int(last_ts)).strftime("%Y-%m-%d %H:%M:%S")
        if last_ts else "—"
    )
    autologin = "Да" if u.get("autologin") == "1" else "Нет"
    attempt_ip = u.get("attempt_ip", "—")

    user_data.append({
        "ID": u.get("userid", "—"),
        "Логин": login,
        "Имя": f"{u.get('name','')} {u.get('surname','')}".strip() or "—",
        "Email": email,
        "Группы": groups or "—",
        "Role ID": role_id,
        "Роль (имя)": role_name,
        "IP последнего входа": attempt_ip,
        "Последний вход": last_login,
        "Автовход": autologin,
        "Язык интерфейса": u.get("lang", "—"),
        "Тема": u.get("theme", "—"),
        "Обновление": u.get("refresh", "—"),
        "Часовой пояс": u.get("timezone", "—")
    })

# === USERGROUPS ===
logger.info("📥 Получаю группы пользователей...")
groups = api.usergroup.get(output=["usrgrpid", "name", "gui_access", "users_status"], selectUsers=["alias", "username"])
logger.info(f"📦 Групп пользователей: {len(groups)}")

group_data = []
for g in groups:
    members = ", ".join(u.get("alias") or u.get("username") or "—" for u in g.get("users", []))
    group_data.append({
        "ID": g.get("usrgrpid"),
        "Группа": g.get("name", "—"),
        "GUI Access": g.get("gui_access", "—"),
        "Статус": g.get("users_status", "—"),
        "Пользователи": members or "—"
    })

# === ROLES ===
logger.info("📥 Получаю роли...")
roles = api.role.get(output=["roleid", "name", "type", "readonly"])
logger.info(f"📦 Ролей: {len(roles)}")

role_data = []
for r in roles:
    role_data.append({
        "ID": r.get("roleid", "—"),
        "Имя роли": r.get("name", "—"),
        "Тип": r.get("type", "—"),
        "Read-only": "Да" if r.get("readonly") == "1" else "Нет"
    })

# === HOSTS ===
logger.info("📥 Получаю список хостов (серверов)...")
hosts = api.host.get(
    output=["hostid", "host", "name", "status"],
    selectInterfaces=["ip", "type", "port", "dns"],
    selectGroups=["name"],
    selectParentTemplates=["name"],
)
logger.info(f"📦 Хостов получено: {len(hosts)}")

host_data = []
for h in hosts:
    ip_list = [i.get("ip") for i in h.get("interfaces", []) if i.get("ip")]
    ip = ", ".join(ip_list) if ip_list else "—"
    groups = ", ".join(g["name"] for g in h.get("groups", [])) or "—"
    templates = ", ".join(t["name"] for t in h.get("parentTemplates", [])) or "—"
    status = "Активен" if str(h.get("status")) == "0" else "Отключён"

    host_data.append({
        "ID": h.get("hostid"),
        "Имя хоста": h.get("name", "—"),
        "Хост (системное имя)": h.get("host", "—"),
        "IP": ip,
        "Группы": groups,
        "Шаблоны": templates,
        "Статус": status
    })

# === СВОДНАЯ ТАБЛИЦА ===
logger.info("📊 Формирую сводку...")
summary_data = [
    ["Дата генерации", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
    ["Пользователей всего", len(user_data)],
    ["С автологином", sum(1 for u in user_data if u["Автовход"] == "Да")],
    ["Групп пользователей", len(group_data)],
    ["Ролей", len(role_data)],
    ["Хостов всего", len(host_data)],
    ["Активных хостов", sum(1 for h in host_data if h["Статус"] == "Активен")],
    ["Отключённых хостов", sum(1 for h in host_data if h["Статус"] == "Отключён")],
]
summary_df = pd.DataFrame(summary_data, columns=["Показатель", "Значение"])

# === СОХРАНЕНИЕ В EXCEL ===
logger.info("💾 Сохраняю всё в Excel...")
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    summary_df.to_excel(writer, sheet_name="Сводка", index=False)
    pd.DataFrame(user_data).sort_values(by="Логин").to_excel(writer, sheet_name="Пользователи", index=False)
    pd.DataFrame(group_data).sort_values(by="Группа").to_excel(writer, sheet_name="Группы", index=False)
    pd.DataFrame(role_data).sort_values(by="Имя роли").to_excel(writer, sheet_name="Роли", index=False)
    pd.DataFrame(host_data).sort_values(by="Имя хоста").to_excel(writer, sheet_name="Хосты", index=False)

logger.info(f"📘 Отчёт сохранён в {OUTPUT_FILE}")

api.logout()
logger.info("🔒 Сессия закрыта. Готово ✅")
