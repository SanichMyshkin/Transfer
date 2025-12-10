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
ALLOWED_DOMAIN = os.getenv("ALLOWED_DOMAIN", "company.ru")

logger = logging.getLogger("zabbix_report")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

if not ZABBIX_URL or not ZABBIX_TOKEN:
    logger.error("Не найден URL или TOKEN. Проверь .env")
    raise SystemExit(1)

api = ZabbixAPI(url=ZABBIX_URL)
api.login(token=ZABBIX_TOKEN)

roles = api.role.get(output=["roleid", "name", "type", "readonly"])
role_name_by_id = {r["roleid"]: r["name"] for r in roles}

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

user_rows = []

groups = api.usergroup.get(
    output=["usrgrpid", "name", "gui_access", "users_status"],
    selectUsers=["alias", "username"],
)

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

hosts = api.host.get(
    output=["hostid", "host", "name", "status"],
    selectInterfaces=["ip", "type", "port", "dns"],
    selectGroups=["name"],
    selectParentTemplates=["name"],
)

batch_size = 500

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

graphs_all = []
for i in range(0, len(hosts), batch_size):
    batch = [h["hostid"] for h in hosts[i : i + batch_size]]
    try:
        part = api.graph.get(output=["graphid"], hostids=batch, selectHosts=["hostid"])
        graphs_all.extend(part)
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

conn_bk = sqlite3.connect(BK_SQLITE_PATH)
conn_bk.row_factory = sqlite3.Row
bk_rows = [dict(r) for r in conn_bk.execute("SELECT * FROM Users").fetchall()]
conn_bk.close()

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

def match_domain(x):
    x = x.lower()
    d = ALLOWED_DOMAIN.lower()
    return x.endswith("@" + d) or x.endswith("." + d)

bk_match_rows = []
tech_rows = []
fired_rows = []

for u in users:
    login = u.get("username") or "—"
    medias = []
    for m in u.get("medias", []):
        s = m.get("sendto")
        if isinstance(s, list):
            medias.extend(s)
        else:
            medias.append(s)

    emails = [e.strip().lower() for e in medias if is_email(e)]
    dom_emails = [e for e in emails if match_domain(e)]

    role_id = u.get("roleid")
    role_name = role_name_by_id.get(role_id, "—")

    base = {
        "ID": u.get("userid", "—"),
        "Логин": login,
        "Имя": f"{u.get('name','')} {u.get('surname','')}".strip() or "—",
        "Emails": emails,
        "Domain emails": dom_emails,
        "Role": role_name,
        "Role ID": role_id,
        "TechFlag": "",
        "FiredFlag": "",
        "Conflict": "",
    }

    if len(dom_emails) == 0:
        base["TechFlag"] = "YES"
        tech_rows.append(base)
        user_rows.append(base)
        continue

    matches = []
    for em in dom_emails:
        if em in bk_by_email:
            matches.extend(bk_by_email[em])

    if len(matches) == 0:
        base["FiredFlag"] = "YES"
        fired_rows.append(base)
        user_rows.append(base)
        continue

    if len(matches) == 1:
        bk_match_rows.append(matches[0])
        user_rows.append(base)
        continue

    base["Conflict"] = "YES"
    user_rows.append(base)
    for r in matches:
        bk_match_rows.append(r)

summary_data = [
    ["Дата генерации", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
    ["Пользователей", len(user_rows)],
    ["Групп пользователей", len(group_rows)],
    ["Хостов", len(host_rows)],
    ["Tech users", len(tech_rows)],
    ["Fired users", len(fired_rows)],
    ["BK matched", len(bk_match_rows)],
]

summary_df = pd.DataFrame(summary_data, columns=["Показатель", "Значение"])

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

api.logout()
