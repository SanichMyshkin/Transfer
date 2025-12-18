import os
import logging
import sqlite3
from datetime import datetime
from collections import Counter

import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI
import psycopg2

load_dotenv()

ZABBIX_URL = os.getenv("ZABBIX_URL")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN")
BK_SQLITE_PATH = os.getenv("BK_SQLITE_PATH")
OUTPUT_FILE = "zabbix_report.xlsx"

# Zabbix DB (PostgreSQL) connection from .env
ZBX_DB_HOST = os.getenv("ZBX_DB_HOST")
ZBX_DB_PORT = int(os.getenv("ZBX_DB_PORT", "5432"))
ZBX_DB_NAME = os.getenv("ZBX_DB_NAME")
ZBX_DB_USER = os.getenv("ZBX_DB_USER")
ZBX_DB_PASSWORD = os.getenv("ZBX_DB_PASSWORD")

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


def fetch_last_online_by_username():
    """
    Returns dict: {username_lower: datetime_of_last_access}
    If DB env vars are not set, returns empty dict and report will have '—'.
    """
    if not all([ZBX_DB_HOST, ZBX_DB_NAME, ZBX_DB_USER, ZBX_DB_PASSWORD]):
        logger.warning(
            "Не заданы параметры Zabbix DB (ZBX_DB_*) в .env — 'Последний онлайн' будет пустым."
        )
        return {}

    sql = """
    select u.username, to_timestamp(ss.lastaccess) as last_online
    from users u
    join lateral (
        select s.lastaccess
        from sessions s
        where u.userid = s.userid
        order by s.lastaccess desc
        limit 1
    ) ss on true
    """

    logger.info("Получаю последний онлайн пользователей из Zabbix DB...")
    conn = psycopg2.connect(
        host=ZBX_DB_HOST,
        port=ZBX_DB_PORT,
        dbname=ZBX_DB_NAME,
        user=ZBX_DB_USER,
        password=ZBX_DB_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    finally:
        conn.close()

    return {
        str(username).lower(): last_online for username, last_online in rows if username
    }


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
        "attempt_ip",
        "autologin",
        "lang",
        "theme",
        "refresh",
        "timezone",
        "rows_per_page",
        "provisioned",
        "ts_provisioned",
        "url",
        "userdirectoryid",
    ],
    selectUsrgrps=["name"],
    selectMedias=["sendto"],
)

# Fetch last online from DB and map by username
last_online_by_username = fetch_last_online_by_username()

user_data = []
for u in users:
    login = u.get("username") or "—"

    # last online (string for Excel)
    last_online_dt = last_online_by_username.get(str(login).lower())
    last_online_str = (
        last_online_dt.strftime("%Y-%m-%d %H:%M:%S") if last_online_dt else "—"
    )

    medias = []
    for m in u.get("medias", []):
        send = m.get("sendto")
        if isinstance(send, list):
            medias.extend(send)
        elif isinstance(send, str):
            medias.append(send)
    email = ", ".join(medias) if medias else "—"

    groups = ", ".join(g["name"] for g in u.get("usrgrps", [])) or "—"
    role_name = role_name_by_id.get(u.get("roleid"), "—")

    user_data.append(
        {
            "ID": u.get("userid"),
            "Логин": login,
            "Имя": f"{u.get('name', '')} {u.get('surname', '')}".strip() or "—",
            "Email": email,
            "Группы": groups,
            "Роль": role_name,
            "Последний онлайн": last_online_str,
            "Автовход": "Да" if u.get("autologin") == "1" else "Нет",
            "Язык": u.get("lang"),
            "Тема": u.get("theme"),
            "Часовой пояс": u.get("timezone"),
        }
    )

logger.info("Получаю группы пользователей...")
groups = api.usergroup.get(
    output=["usrgrpid", "name", "gui_access", "users_status"],
    selectUsers=["alias", "username"],
)

group_data = []
for g in groups:
    members = ", ".join(
        u.get("alias") or u.get("username") or "—" for u in g.get("users", [])
    )
    group_data.append(
        {
            "ID": g.get("usrgrpid"),
            "Группа": g.get("name"),
            "Статус": g.get("users_status"),
            "Пользователи": members or "—",
        }
    )

logger.info("Подключаюсь к BK SQLite...")
bk_conn = sqlite3.connect(BK_SQLITE_PATH)
bk_conn.row_factory = sqlite3.Row
bk_rows = bk_conn.execute("SELECT * FROM bk").fetchall()
bk_conn.close()

bk_users = [dict(r) for r in bk_rows]
bk_logins = {(u.get("sAMAccountName") or "").lower(): u for u in bk_users}

matched_bk_users = []
techfired_users = []

for u in users:
    login = (u.get("username") or "").lower()
    if login in bk_logins:
        matched_bk_users.append(bk_logins[login])
    else:
        techfired_users.append(u)

logger.info("Получаю хосты...")
hosts = api.host.get(
    output=["hostid", "host", "name", "status"],
    selectInterfaces=["ip", "dns"],
    selectGroups=["name"],
    selectParentTemplates=["name"],
    selectTags="extend",
)

batch_size = 500
triggers_all = []
graphs_all = []

for i in range(0, len(hosts), batch_size):
    batch = [h["hostid"] for h in hosts[i : i + batch_size]]
    try:
        triggers_all.extend(
            api.trigger.get(output=["triggerid"], hostids=batch, selectHosts=["hostid"])
        )
    except Exception:
        pass

    try:
        graphs_all.extend(
            api.graph.get(output=["graphid"], hostids=batch, selectHosts=["hostid"])
        )
    except Exception:
        pass

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
    for h in hosts:
        if h.get("name") in d.get("name", ""):
            dashboard_count[h["hostid"]] += 1


def get_tag_value(tags, tag_name):
    for t in tags or []:
        if t.get("tag") == tag_name:
            return t.get("value")
    return None


hosts_with_owner = []
hosts_without_owner = []

for h in hosts:
    hostid = h.get("hostid")

    ip_list = [i.get("ip") for i in h.get("interfaces", []) if i.get("ip")]
    ip = ", ".join(ip_list) if ip_list else "—"

    owner = get_tag_value(h.get("tags"), "host_owner")

    row = {
        "ID": hostid,
        "Имя хоста": h.get("name", "—"),
        "Системное имя": h.get("host", "—"),
        "IP": ip,
        "Группы": ", ".join(g["name"] for g in h.get("groups", [])) or "—",
        "Шаблоны": ", ".join(t["name"] for t in h.get("parentTemplates", [])) or "—",
        "Триггеров": trigger_count.get(hostid, 0),
        "Графиков": graph_count.get(hostid, 0),
        "Дашбордов": dashboard_count.get(hostid, 0),
        "Статус": "Активен" if str(h.get("status")) == "0" else "Отключён",
    }

    if owner:
        row["Владелец"] = owner
        hosts_with_owner.append(row)
    else:
        hosts_without_owner.append(row)

summary_data = [
    ["Дата формирования", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
    ["Хостов всего", len(hosts_with_owner) + len(hosts_without_owner)],
    ["Хостов с владельцем", len(hosts_with_owner)],
    ["Хостов без владельца", len(hosts_without_owner)],
    [
        "Хостов активных",
        sum(
            1
            for h in hosts_with_owner + hosts_without_owner
            if h["Статус"] == "Активен"
        ),
    ],
    [
        "Хостов отключённых",
        sum(
            1
            for h in hosts_with_owner + hosts_without_owner
            if h["Статус"] == "Отключён"
        ),
    ],
]

summary_df = pd.DataFrame(summary_data, columns=["Показатель", "Значение"])

logger.info("Сохраняю Excel...")

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    pd.DataFrame(user_data).sort_values(by="Логин").to_excel(
        writer, sheet_name="Пользователи", index=False
    )
    pd.DataFrame(group_data).sort_values(by="Группа").to_excel(
        writer, sheet_name="Группы пользователей", index=False
    )
    pd.DataFrame(matched_bk_users).to_excel(
        writer, sheet_name="Пользователи BK", index=False
    )
    pd.DataFrame(techfired_users).to_excel(
        writer, sheet_name="Не найдено в BK", index=False
    )
    pd.DataFrame(hosts_with_owner).sort_values(by="Имя хоста").to_excel(
        writer, sheet_name="Хосты с владельцем", index=False
    )
    pd.DataFrame(hosts_without_owner).sort_values(by="Имя хоста").to_excel(
        writer, sheet_name="Хосты без владельца", index=False
    )
    summary_df.to_excel(writer, sheet_name="Сводка", index=False)

logger.info(f"Отчёт сохранён: {OUTPUT_FILE}")

api.logout()
logger.info("Сессия закрыта.")
