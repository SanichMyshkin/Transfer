import os
import logging
import sqlite3
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from collections import Counter
from zabbix_utils import ZabbixAPI

load_dotenv()
ZABBIX_URL = os.getenv("ZABBIX_URL")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN")
BK_SQLITE_PATH = os.getenv("BK_SQLITE_PATH")
OUTPUT_FILE = "zabbix_full_report.xlsx"

logger = logging.getLogger("zabbix_report")
logger.setLevel(logging.INFO)
fmt = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"
)
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

if not ZABBIX_URL or not ZABBIX_TOKEN:
    logger.error("Не найден URL или TOKEN. Проверь .env")
    raise SystemExit(1)

logger.info("Подключаюсь к Zabbix...")
api = ZabbixAPI(url=ZABBIX_URL)
api.login(token=ZABBIX_TOKEN)
logger.info("Успешно!")

logger.info("Получаю роли...")
roles = api.role.get(output=["roleid", "name", "type", "readonly"])
role_name_by_id = {r["roleid"]: r["name"] for r in roles}

logger.info("Получаю пользователей...")
users = api.user.get(
    output=[
        "userid",
        "username",
        "name",
        "surname",
        "roleid",
        "attempt_clock",
        "attempt_failed",
        "attempt_ip",
        "autologin",
        "autologout",
        "lang",
        "provisioned",
        "refresh",
        "rows_per_page",
        "theme",
        "ts_provisioned",
        "url",
        "userdirectoryid",
        "timezone",
    ],
    selectUsrgrps=["name"],
    selectMedias=["sendto"],
    selectSessions=["lastaccess"],
)

user_data = []
for u in users:
    login = u.get("username") or "—"

    medias = []
    for m in u.get("medias", []):
        send = m.get("sendto")
        if isinstance(send, list):
            medias.extend(send)
        elif isinstance(send, str):
            medias.append(send)
    email = ", ".join(medias) if medias else "—"

    groups = ", ".join(g["name"] for g in u.get("usrgrps", [])) or "—"

    role_id = u.get("roleid")
    role_name = role_name_by_id.get(role_id, "—")

    user_data.append(
        {
            "ID": u.get("userid", "—"),
            "Логин": login,
            "Имя": f"{u.get('name', '')} {u.get('surname', '')}".strip() or "—",
            "Email": email,
            "Группы": groups,
            "Role ID": role_id,
            "Роль": role_name,
            "IP last unsuccessful login": u.get("attempt_ip", "—"),
            "Автовход": "Да" if u.get("autologin") == "1" else "Нет",
            "Язык интерфейса": u.get("lang", "—"),
            "Тема": u.get("theme", "—"),
            "Обновление": u.get("refresh", "—"),
            "Часовой пояс": u.get("timezone", "—"),
            "Rows per page": u.get("rows_per_page", "—"),
            "Provisioned": u.get("provisioned", "—"),
            "TS Provisioned": u.get("ts_provisioned", "—"),
            "URL": u.get("url", "—"),
            "User Directory ID": u.get("userdirectoryid", "—"),
        }
    )

logger.info("Подключаюсь к BK SQLite...")
bk_conn = sqlite3.connect(BK_SQLITE_PATH)
bk_conn.row_factory = sqlite3.Row
bk_rows = bk_conn.execute("SELECT * FROM Users").fetchall()
bk_conn.close()

bk_users_rows = [dict(r) for r in bk_rows]
bk_logins = {(u.get("sAMAccountName") or "").strip().lower(): u for u in bk_users_rows}

matched_bk_users = []
techfired_users = []

logger.info("Сверка пользователей по username ↔ sAMAccountName...")

for u in users:
    zbx_login = (u.get("username") or "").strip().lower()
    if zbx_login in bk_logins:
        matched_bk_users.append(bk_logins[zbx_login])
    else:
        techfired_users.append(u)

logger.info(f"Найдено в BK: {len(matched_bk_users)}")
logger.info(f"Не найдено в BK (tech/fired): {len(techfired_users)}")

logger.info("Получаю хосты...")
hosts = api.host.get(
    output=["hostid", "host", "name", "status"],
    selectInterfaces=["ip", "type", "port", "dns"],
    selectGroups=["name"],
    selectParentTemplates=["name"],
)

batch_size = 500

logger.info("Получаю триггеры...")
triggers_all = []
for i in range(0, len(hosts), batch_size):
    batch = [h["hostid"] for h in hosts[i : i + batch_size]]
    try:
        part = api.trigger.get(
            output=["triggerid"], hostids=batch, selectHosts=["hostid"]
        )
        triggers_all.extend(part)
    except Exception:
        pass

logger.info("Получаю графики...")
graphs_all = []
for i in range(0, len(hosts), batch_size):
    batch = [h["hostid"] for h in hosts[i : i + batch_size]]
    try:
        part = api.graph.get(output=["graphid"], hostids=batch, selectHosts=["hostid"])
        graphs_all.extend(part)
    except Exception:
        pass

logger.info("Получаю дашборды...")
try:
    dashboards_all = api.dashboard.get(output=["dashboardid", "name"])
except Exception:
    dashboards_all = []

trigger_count = Counter()
for t in triggers_all:
    for h in t.get("hosts", []):
        trigger_count[h["hostid"]] += 1

graph_count = Counter()
for g in graphs_all:
    for h in g.get("hosts", []):
        graph_count[h["hostid"]] += 1

dashboard_count = Counter()
for d in dashboards_all:
    name = d.get("name", "")
    for h in hosts:
        if h.get("name") in name:
            dashboard_count[h["hostid"]] += 1

host_data = []
for h in hosts:
    hostid = h.get("hostid")
    ip_list = [i.get("ip") for i in h.get("interfaces", []) if i.get("ip")]
    ip = ", ".join(ip_list) if ip_list else "—"

    host_data.append(
        {
            "ID": hostid,
            "Имя хоста": h.get("name", "—"),
            "Хост": h.get("host", "—"),
            "IP": ip,
            "Группы": ", ".join(g["name"] for g in h.get("groups", [])) or "—",
            "Шаблоны": ", ".join(t["name"] for t in h.get("parentTemplates", []))
            or "—",
            "Триггеров": trigger_count.get(hostid, 0),
            "Графиков": graph_count.get(hostid, 0),
            "Дашбордов": dashboard_count.get(hostid, 0),
            "Статус": "Активен" if str(h.get("status")) == "0" else "Отключён",
        }
    )

summary_data = [
    ["Дата генерации", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
    ["Пользователей", len(user_data)],
    ["Групп пользователей", len(bk_users_rows)],
    ["Хостов", len(host_data)],
    ["Хостов активных", sum(1 for h in host_data if h["Статус"] == "Активен")],
    ["Хостов отключённых", sum(1 for h in host_data if h["Статус"] == "Отключён")],
    ["Триггеров", sum(trigger_count.values())],
    ["Графиков", sum(graph_count.values())],
    ["Дашбордов", sum(dashboard_count.values())],
]

summary_df = pd.DataFrame(summary_data, columns=["Показатель", "Значение"])

logger.info("Сохраняю Excel...")

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    pd.DataFrame(user_data).sort_values(by="Логин").to_excel(
        writer, sheet_name="Пользователи", index=False
    )
    pd.DataFrame(matched_bk_users).to_excel(writer, sheet_name="BK_Users", index=False)
    pd.DataFrame(techfired_users).to_excel(
        writer, sheet_name="TechFired_Users", index=False
    )
    pd.DataFrame(host_data).sort_values(by="Имя хоста").to_excel(
        writer, sheet_name="Хосты", index=False
    )
    summary_df.to_excel(writer, sheet_name="Сводка", index=False)

logger.info(f"Отчёт сохранён: {OUTPUT_FILE}")

api.logout()
logger.info("Сессия закрыта.")
