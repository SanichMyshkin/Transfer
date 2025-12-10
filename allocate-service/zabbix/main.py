import os
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

load_dotenv()
ZABBIX_URL = os.getenv("ZABBIX_URL")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN")
OUTPUT_FILE = "zabbix_full_report.xlsx"

logger = logging.getLogger("zabbix_report")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(message)s", "%H:%M:%S")
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

if not ZABBIX_URL or not ZABBIX_TOKEN:
    raise SystemExit(1)

logger.info("Подключение к Zabbix...")
api = ZabbixAPI(url=ZABBIX_URL)
api.login(token=ZABBIX_TOKEN)
logger.info("OK")

logger.info("Получаю пользователей...")
users = api.user.get(
    output=[
        "userid", "username", "name", "surname", "roleid",
        "attempt_clock", "attempt_failed", "attempt_ip",
        "autologin", "autologout", "lang", "provisioned",
        "refresh", "rows_per_page", "theme", "ts_provisioned",
        "url", "userdirectoryid", "timezone"
    ],
    selectUsrgrps=["name"],
    selectMedias=["sendto"],
    selectSessions=["lastaccess"]
)
logger.info(f"Пользователей: {len(users)}")

logger.info("Загружаю роли...")
roles = api.role.get(output=["roleid", "name"])
role_map = {r["roleid"]: r["name"] for r in roles}

user_data = []
for u in users:
    medias = []
    for m in u.get("medias", []):
        v = m.get("sendto")
        if v:
            medias.append(v)
    email = ", ".join(medias) if medias else "—"

    groups = ", ".join(g["name"] for g in u.get("usrgrps", [])) or "—"
    role_name = role_map.get(u.get("roleid"), "—")

    sessions = u.get("sessions", [])
    if sessions:
        ts = sessions[0].get("lastaccess")
        last_login = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S") if ts else "—"
    else:
        last_login = "—"

    user_data.append({
        "ID": u.get("userid"),
        "Логин": u.get("username", "—"),
        "Имя": f"{u.get('name','')} {u.get('surname','')}".strip() or "—",
        "Email": email,
        "Группы": groups,
        "Роль": role_name,
        "Последний вход": last_login,
        "IP последней попытки": u.get("attempt_ip", "—"),
        "Неудачные попытки": u.get("attempt_failed", "0"),
        "Автовход": u.get("autologin", "0"),
        "Автовыход": u.get("autologout", "0"),
        "Язык": u.get("lang", "—"),
        "Тема": u.get("theme", "—"),
        "Обновление": u.get("refresh", "—"),
        "Часовой пояс": u.get("timezone", "—"),
        "Provisioned": u.get("provisioned", "—"),
        "ts_provisioned": u.get("ts_provisioned", "—"),
        "URL": u.get("url", "—"),
        "UserDirectoryID": u.get("userdirectoryid", "—"),
        "Rows per page": u.get("rows_per_page", "—")
    })

logger.info("Загружаю группы пользователей...")
groups = api.usergroup.get(
    output=["usrgrpid", "name", "gui_access", "users_status"],
    selectUsers=["username"]
)
logger.info(f"Групп: {len(groups)}")

group_data = []
for g in groups:
    members = ", ".join(u.get("username") for u in g.get("users", []))
    group_data.append({
        "ID": g.get("usrgrpid"),
        "Группа": g.get("name", "—"),
        "GUI Access": g.get("gui_access", "—"),
        "Статус": g.get("users_status", "—"),
        "Пользователи": members or "—"
    })

summary_data = [
    ["Дата", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
    ["Пользователей", len(user_data)],
    ["Групп пользователей", len(group_data)],
]

summary_df = pd.DataFrame(summary_data, columns=["Показатель", "Значение"])

logger.info("Сохраняю Excel...")
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    summary_df.to_excel(writer, sheet_name="Сводка", index=False)
    pd.DataFrame(user_data).sort_values(by="Логин").to_excel(writer, sheet_name="Пользователи", index=False)
    pd.DataFrame(group_data).sort_values(by="Группа").to_excel(writer, sheet_name="Группы", index=False)

api.logout()
logger.info("Готово.")
