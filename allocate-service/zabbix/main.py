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


def _zbx_db_conn():
    if not all([ZBX_DB_HOST, ZBX_DB_NAME, ZBX_DB_USER, ZBX_DB_PASSWORD]):
        raise RuntimeError("Не заданы параметры Zabbix DB (ZBX_DB_*) в .env")
    return psycopg2.connect(
        host=ZBX_DB_HOST,
        port=ZBX_DB_PORT,
        dbname=ZBX_DB_NAME,
        user=ZBX_DB_USER,
        password=ZBX_DB_PASSWORD,
    )


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
    conn = _zbx_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    finally:
        conn.close()

    return {
        str(username).lower(): last_online for username, last_online in rows if username
    }


def fetch_internal_usernames(group_name: str = "ZabbixInternalUsers"):
    """
    Returns set of usernames (lowercase) that are in internal group (НЕ LDAP по вашему правилу).
    """
    if not all([ZBX_DB_HOST, ZBX_DB_NAME, ZBX_DB_USER, ZBX_DB_PASSWORD]):
        logger.warning(
            "Не заданы параметры Zabbix DB (ZBX_DB_*) в .env — классификация fired/tech будет без LDAP-логики."
        )
        return set()

    sql = """
    select u.username
    from users u
    join users_groups ug on u.userid = ug.userid
    join usrgrp g on g.usrgrpid = ug.usrgrpid
    where g.name = %s
    """

    logger.info(f"Получаю НЕ-LDAP пользователей из группы '{group_name}' (для tech)...")
    conn = _zbx_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (group_name,))
            rows = cur.fetchall()
    finally:
        conn.close()

    return {str(r[0]).lower() for r in rows if r and r[0]}


def get_tag_value(tags, tag_name):
    for t in tags or []:
        if t.get("tag") == tag_name:
            return t.get("value")
    return None

def get_first_tag_value(tags, tag_names):
    """Возвращает первое найденное значение по списку имён тегов (в порядке приоритета)."""
    for name in tag_names:
        v = get_tag_value(tags, name)
        if v:
            return v
    return None


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

last_online_by_username = fetch_last_online_by_username()

user_data = []
for u in users:
    login = u.get("username") or "—"

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

user_row_by_login = {str(r["Логин"]).lower(): r for r in user_data if r.get("Логин")}

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
not_found_in_bk_users = []
internal_usernames = fetch_internal_usernames("ZabbixInternalUsers")

for u in users:
    login_lower = (u.get("username") or "").lower()
    if not login_lower:
        continue

    if login_lower in bk_logins:
        matched_bk_users.append(bk_logins[login_lower])
    else:
        utype = "tech" if login_lower in internal_usernames else "fired"

        base_row = user_row_by_login.get(
            login_lower, {"Логин": u.get("username") or "—"}
        )
        row = {"Тип": utype}
        row.update(base_row)
        not_found_in_bk_users.append(row)


logger.info("Получаю хосты...")
hosts = api.host.get(
    output=["hostid", "host", "name", "status"],
    selectInterfaces=["ip", "dns"],
    selectGroups=["name"],
    selectParentTemplates=["templateid", "name"],
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
            api.graph.get(
                output=["graphid"],
                hostids=batch,
                selectHosts="extend",
            )
        )
    except Exception:
        pass

logger.info(f"Graphs returned: {len(graphs_all)}")
if graphs_all:
    logger.info(f"First graph keys: {list(graphs_all[0].keys())}")
    logger.info(f"First graph hosts: {graphs_all[0].get('hosts')}")

trigger_count = Counter()
for t in triggers_all:
    for h in t.get("hosts", []):
        trigger_count[str(h["hostid"])] += 1

graph_count = Counter()
for g in graphs_all:
    for h in g.get("hosts", []):
        graph_count[str(h["hostid"])] += 1


logger.info("Считаю host dashboards (template dashboards)...")

hostids_by_templateid = {}
for h in hosts:
    hostid = str(h.get("hostid"))
    for t in h.get("parentTemplates", []) or []:
        tid = t.get("templateid")
        if tid:
            hostids_by_templateid.setdefault(str(tid), set()).add(hostid)

all_templateids = list(hostids_by_templateid.keys())
host_dashboards_count = Counter()
if not all_templateids:
    logger.warning(
        "У хостов не найдено parentTemplates.templateid — дашборды посчитать нельзя."
    )
else:
    try:
        tds = api.templatedashboard.get(
            output=["dashboardid", "name", "templateid"],
            templateids=all_templateids,
        )
        logger.info(f"Template dashboards returned: {len(tds)}")

        dashboards_count_by_templateid = Counter()
        for d in tds:
            tid = str(d.get("templateid"))
            dashboards_count_by_templateid[tid] += 1

        for tid, hostid_set in hostids_by_templateid.items():
            cnt = dashboards_count_by_templateid.get(tid, 0)
            if cnt:
                for hostid in hostid_set:
                    host_dashboards_count[hostid] += cnt

    except Exception as e:
        logger.warning(f"templatedashboard.get не сработал (права/версия/обёртка): {e}")

logger.info("Host dashboards подсчитаны.")

hosts_with_owner = []
hosts_without_owner = []

for h in hosts:
    hostid = h.get("hostid")

    ip_list = [i.get("ip") for i in h.get("interfaces", []) if i.get("ip")]
    ip = ", ".join(ip_list) if ip_list else "—"

    # Новый формат тегов:
    owner_name = get_first_tag_value(h.get("tags"), ["host_owner_name", "host_owner"])
    owner_id = get_tag_value(h.get("tags"), "host_owner_id")

    row = {
        "ID": hostid,
        "Имя хоста": h.get("name", "—"),
        "Системное имя": h.get("host", "—"),
        "IP": ip,
        "Группы": ", ".join(g["name"] for g in h.get("groups", [])) or "—",
        "Шаблоны": ", ".join(t["name"] for t in h.get("parentTemplates", [])) or "—",
        "Триггеров": trigger_count.get(str(hostid), 0),
        "Графиков": graph_count.get(str(hostid), 0),
        "Дашбордов": host_dashboards_count.get(str(hostid), 0),
        "Статус": "Активен" if str(h.get("status")) == "0" else "Отключён",
    }

    if owner_name:
        row["Владелец"] = owner_name
        row["ID владельца"] = owner_id or "—"
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
            for hh in hosts_with_owner + hosts_without_owner
            if hh["Статус"] == "Активен"
        ),
    ],
    [
        "Хостов отключённых",
        sum(
            1
            for hh in hosts_with_owner + hosts_without_owner
            if hh["Статус"] == "Отключён"
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

    df_nf = pd.DataFrame(not_found_in_bk_users)
    if not df_nf.empty:
        cols = ["Тип"] + [c for c in df_nf.columns if c != "Тип"]
        df_nf = df_nf[cols].sort_values(by=["Тип", "Логин"], kind="stable")
    df_nf.to_excel(writer, sheet_name="Уволенные и тех учетки", index=False)

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
