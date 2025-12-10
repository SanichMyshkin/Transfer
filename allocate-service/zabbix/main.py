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
OUTPUT_FILE = "zabbix_full_report.xlsx"
BK_SQLITE_PATH = os.getenv("BK_SQLITE_PATH")
ALLOWED_DOMAINS_RAW = os.getenv("ALLOWED_DOMAINS", "company.ru")

ALLOWED_DOMAINS = [
    d.strip().lower() for d in ALLOWED_DOMAINS_RAW.split(",") if d.strip()
]

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

if not BK_SQLITE_PATH:
    logger.error("Не указан BK_SQLITE_PATH в .env")
    raise SystemExit(1)

logger.info("Подключаюсь к Zabbix...")
api = ZabbixAPI(url=ZABBIX_URL)
api.login(token=ZABBIX_TOKEN)
logger.info("Подключение к Zabbix успешно")

logger.info("Получаю роли...")
roles = api.role.get(output=["roleid", "name", "type", "readonly"])
logger.info(f"Ролей: {len(roles)}")
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
logger.info(f"Пользователей: {len(users)}")

logger.info("Получаю группы пользователей...")
groups = api.usergroup.get(
    output=["usrgrpid", "name", "gui_access", "users_status"],
    selectUsers=["alias", "username"],
)
logger.info(f"Групп пользователей: {len(groups)}")

group_rows = []
for g in groups:
    members = ", ".join(
        u.get("alias") or u.get("username") or "—" for u in g.get("users", [])
    )
    group_rows.append(
        {
            "ID": g.get("usrgrpid"),
            "Группа": g.get("name", "—"),
            "GUI Access": g.get("gui_access", "—"),
            "Статус": g.get("users_status", "—"),
            "Пользователи": members or "—",
        }
    )

logger.info("Получаю хосты...")
hosts = api.host.get(
    output=["hostid", "host", "name", "status"],
    selectInterfaces=["ip", "type", "port", "dns"],
    selectGroups=["name"],
    selectParentTemplates=["name"],
)
logger.info(f"Хостов: {len(hosts)}")

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
        logger.info(f"Триггеры: +{len(part)} (всего {len(triggers_all)})")
    except Exception as e:
        logger.warning(f"Ошибка при получении триггеров для пачки {i}: {e}")
logger.info(f"Всего триггеров: {len(triggers_all)}")

logger.info("Получаю графики...")
graphs_all = []
for i in range(0, len(hosts), batch_size):
    batch = [h["hostid"] for h in hosts[i : i + batch_size]]
    try:
        part = api.graph.get(output=["graphid"], hostids=batch, selectHosts=["hostid"])
        graphs_all.extend(part)
        logger.info(f"Графики: +{len(part)} (всего {len(graphs_all)})")
    except Exception as e:
        logger.warning(f"Ошибка при получении графиков для пачки {i}: {e}")
logger.info(f"Всего графиков: {len(graphs_all)}")

logger.info("Получаю дашборды...")
try:
    dashboards_all = api.dashboard.get(output=["dashboardid", "name"])
    logger.info(f"Дашбордов: {len(dashboards_all)}")
except Exception as e:
    logger.warning(f"Ошибка при получении дашбордов: {e}")
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

host_rows = []
for h in hosts:
    hostid = h.get("hostid")
    ip_list = [i.get("ip") for i in h.get("interfaces", []) if i.get("ip")]
    ip = ", ".join(ip_list) if ip_list else "—"
    host_rows.append(
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

logger.info("Открываю БК SQLite...")
conn_bk = sqlite3.connect(BK_SQLITE_PATH)
conn_bk.row_factory = sqlite3.Row
bk_rows = [dict(r) for r in conn_bk.execute("SELECT * FROM Users").fetchall()]
conn_bk.close()
logger.info(f"Пользователей в БК: {len(bk_rows)}")

bk_by_email = {}
for r in bk_rows:
    em = (r.get("Email") or "").strip().lower()
    if em:
        bk_by_email.setdefault(em, []).append(r)

def is_email(x):
    if not isinstance(x, str):
        return False
    if "@" not in x:
        return False
    if " " in x:
        return False
    if x.startswith("+"):
        return False
    return True

def match_domain(email):
    e = email.lower()
    for d in ALLOWED_DOMAINS:
        d = d.lower()
        if e.endswith("@" + d) or e.endswith("." + d):
            return True
    return False

user_rows = []
bk_match_rows = []
tech_rows = []
fired_rows = []

logger.info("Сопоставляю пользователей Zabbix с БК...")

for u in users:
    login = u.get("username") or "—"

    medias = []
    for m in u.get("medias", []):
        s = m.get("sendto")
        if isinstance(s, list):
            medias.extend(s)
        elif s is not None:
            medias.append(s)

    emails = [e.strip().lower() for e in medias if is_email(e)]
    dom_emails = [e for e in emails if match_domain(e)]

    role_id = u.get("roleid")
    role_name = role_name_by_id.get(role_id, "—")

    base = {
        "ID": u.get("userid", "—"),
        "Логин": login,
        "Имя": f"{u.get('name', '')} {u.get('surname', '')}".strip() or "—",
        "Emails": ", ".join(emails) if emails else "",
        "Domain emails": ", ".join(dom_emails) if dom_emails else "",
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
        "TechFlag": "",
        "FiredFlag": "",
        "Conflict": "",
    }

    if len(dom_emails) == 0:
        base["TechFlag"] = "YES"
        tech_rows.append(base)
        user_rows.append(base)
        logger.info(f"TECH (нет доменных email): {login}")
        continue

    matches = []
    for em in dom_emails:
        if em in bk_by_email:
            matches.extend(bk_by_email[em])

    if len(matches) == 0:
        base["FiredFlag"] = "YES"
        fired_rows.append(base)
        user_rows.append(base)
        logger.info(f"FIRED (нет в БК): {login} / {dom_emails}")
        continue

    if len(matches) == 1:
        bk_match_rows.append(matches[0])
        user_rows.append(base)
        logger.info(f"ACTIVE (1 совпадение в БК): {login} / {dom_emails}")
        continue

    base["Conflict"] = "YES"
    user_rows.append(base)
    logger.warning(
        f"CONFLICT (несколько совпадений в БК): {login} / {dom_emails} "
        f"/ BK records: {len(matches)}"
    )
    for r in matches:
        bk_match_rows.append(r)

logger.info(f"Tech учёток: {len(tech_rows)}")
logger.info(f"FIRED учёток: {len(fired_rows)}")
logger.info(f"Совпало с БК записей: {len(bk_match_rows)}")

summary_data = [
    ["Дата генерации", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
    ["Пользователей (Zabbix)", len(user_rows)],
    ["Групп пользователей", len(group_rows)],
    ["Хостов", len(host_rows)],
    ["Tech users", len(tech_rows)],
    ["Fired users", len(fired_rows)],
    ["BK matched", len(bk_match_rows)],
]

summary_df = pd.DataFrame(summary_data, columns=["Показатель", "Значение"])

logger.info("Сохраняю Excel отчёт...")

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    pd.DataFrame(user_rows).sort_values(by="Логин").to_excel(
        writer, sheet_name="Пользователи", index=False
    )
    pd.DataFrame(group_rows).sort_values(by="Группа").to_excel(
        writer, sheet_name="Группы", index=False
    )
    pd.DataFrame(host_rows).sort_values(by="Имя хоста").to_excel(
        writer, sheet_name="Хосты", index=False
    )
    pd.DataFrame(bk_match_rows).to_excel(
        writer, sheet_name="BK_Users", index=False
    )
    tf = pd.DataFrame(tech_rows + fired_rows)
    tf.to_excel(writer, sheet_name="Tech_And_Fired", index=False)
    summary_df.to_excel(writer, sheet_name="Сводка", index=False)

logger.info(f"Отчёт сохранён: {OUTPUT_FILE}")

api.logout()
logger.info("Сессия Zabbix закрыта")
