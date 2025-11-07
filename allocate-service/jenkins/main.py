import os
import sys
import ssl
import time
import logging
import urllib3
import xlsxwriter
from datetime import datetime
from dotenv import load_dotenv
from ldap3 import Server, Connection, ALL, SUBTREE, Tls
from ldap3.utils.conv import escape_filter_chars
from jenkins_groovy import JenkinsGroovyClient
from jenkins_scripts import SCRIPT_USERS, SCRIPT_JOBS, SCRIPT_NODES, SCRIPT_AD_GROUP


# === Логирование ===
LOG_FILE = os.path.join(os.getcwd(), "jenkins_inventory.log")

for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# === Jenkins ===
JENKINS_URL = os.getenv("JENKINS_URL")
USER = os.getenv("USER")
TOKEN = os.getenv("TOKEN")
FILE_PATH = os.path.join(os.getcwd(), "jenkins_inventory.xlsx")

client = JenkinsGroovyClient(JENKINS_URL, USER, TOKEN, is_https=False)

# === LDAP ===
AD_SERVER = os.getenv("AD_SERVER")
AD_USER = os.getenv("AD_USER")
AD_PASSWORD = os.getenv("AD_PASSWORD")
AD_BASE = os.getenv("AD_PEOPLE_SEARCH_BASE")
CA_CERT = os.getenv("CA_CERT", "CA.crt")


# ============================================================
# === Jenkins Data ===
# ============================================================

def get_users():
    logger.info("Получаем пользователей...")
    data = client.run_script(SCRIPT_USERS)
    logger.info(f"Пользователей: {data['total']}")
    return data


def get_jobs():
    logger.info("Получаем джобы...")
    data = client.run_script(SCRIPT_JOBS)
    logger.info(f"Джоб: {data['total']}")
    return data


def get_nodes():
    logger.info("Получаем ноды...")
    data = client.run_script(SCRIPT_NODES)
    logger.info(f"Нод: {data['total']}")
    return data


def get_ad_groups():
    logger.info("Получаем AD-группы из Jenkins...")
    data = client.run_script(SCRIPT_AD_GROUP)
    groups = data.get("ad_groups", [])
    logger.info(f"Найдено AD-групп: {len(groups)}")
    return groups


# ============================================================
# === LDAP ===
# ============================================================

def connect_ldap():
    logger.info(f"Подключаемся к LDAP: {AD_SERVER}")
    tls = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=CA_CERT)
    server = Server(AD_SERVER, use_ssl=True, get_info=ALL, tls=tls)
    return Connection(server, AD_USER, AD_PASSWORD, auto_bind=True)


def safe_get(attr_dict, key):
    val = attr_dict.get(key, "")
    if isinstance(val, (list, tuple)):
        return val[0] if val else ""
    return val or ""


def get_users_from_ad_group(conn, group_name):
    start_time = time.perf_counter()
    name_esc = escape_filter_chars(group_name)
    group_filter = f"(&(objectClass=group)(|(cn={name_esc})(sAMAccountName={name_esc})(name={name_esc})))"

    logger.info(f"🔍 Поиск группы в LDAP: {group_name}")
    conn.search(
        search_base=AD_BASE,
        search_filter=group_filter,
        search_scope=SUBTREE,
        attributes=["distinguishedName", "cn", "member"],
    )

    if not conn.entries:
        logger.warning(f"⚠️ Группа '{group_name}' не найдена в AD.")
        return {"group": group_name, "found": False, "members": []}

    entry = conn.entries[0]
    members = entry.member.values if "member" in entry else []
    users = []

    if not members:
        logger.info(f"ℹ️ Группа '{group_name}' пуста.")
        return {"group": group_name, "found": True, "members": []}

    for dn in members:
        try:
            conn.search(
                search_base=dn,
                search_filter="(objectClass=user)",
                search_scope=SUBTREE,
                attributes=["sAMAccountName", "displayName", "mail", "whenCreated"],
            )
            if conn.entries:
                u = conn.entries[0]
                a = u.entry_attributes_as_dict
                users.append({
                    "ad_group": group_name,
                    "user": safe_get(a, "sAMAccountName"),
                    "displayName": safe_get(a, "displayName"),
                    "mail": safe_get(a, "mail").lower(),
                    "whenCreated": str(safe_get(a, "whenCreated")),
                    "user_dn": dn,
                })
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке DN {dn} в группе '{group_name}': {e}")

    elapsed = time.perf_counter() - start_time
    logger.info(f"✅ Группа '{group_name}' обработана: {len(users)} пользователей, {elapsed:.2f} сек.")
    return {"group": group_name, "found": True, "members": users}


def fetch_ldap_group_members():
    groups = get_ad_groups()
    conn = connect_ldap()
    ad_group_members = []
    total_groups = len(groups)
    logger.info(f"=== Обработка {total_groups} AD-групп из Jenkins ===")

    for idx, group in enumerate(groups, start=1):
        logger.info(f"\n[{idx}/{total_groups}] ▶️ Обрабатывается AD-группа: {group}")
        try:
            g_data = get_users_from_ad_group(conn, group)
            if g_data["found"]:
                ad_group_members.extend(g_data["members"])
        except Exception as e:
            logger.error(f"❌ Ошибка при запросе группы '{group}': {e}")

    conn.unbind()
    logger.info(f"🎯 Завершено. Всего пользователей из всех групп: {len(ad_group_members)}")
    return ad_group_members


# ============================================================
# === Сопоставление Jenkins ↔ AD ===
# ============================================================

def match_jenkins_to_ad(jenkins_users, ad_group_members):
    ad_by_email, ad_by_user = {}, {}
    for a in ad_group_members:
        mail = a.get("mail", "").lower()
        user = a.get("user", "").lower()
        group = a.get("ad_group", "")
        if mail:
            ad_by_email.setdefault(mail, []).append(group)
        if user:
            ad_by_user.setdefault(user, []).append(group)

    matches, not_found = [], []
    for u in jenkins_users["users"]:
        jid = u.get("id", "").lower()
        mail = (u.get("email") or "").lower()
        matched_groups = set()
        if mail in ad_by_email:
            matched_groups.update(ad_by_email[mail])
        if jid in ad_by_user:
            matched_groups.update(ad_by_user[jid])

        if matched_groups:
            matches.append({
                "jenkins_id": u.get("id", ""),
                "fullName": u.get("fullName", ""),
                "email": u.get("email", ""),
                "ad_groups": ", ".join(sorted(matched_groups))
            })
        else:
            not_found.append({
                "jenkins_id": u.get("id", ""),
                "fullName": u.get("fullName", ""),
                "email": u.get("email", ""),
                "ad_groups": "NOT FOUND"
            })

    logger.info(f"🧩 Совпадений найдено: {len(matches)}, не найдено: {len(not_found)}")
    return matches + not_found


# ============================================================
# === Excel Writer ===
# ============================================================

def write_excel(users, jobs, nodes, ad_group_members, user_ad_match):
    wb = xlsxwriter.Workbook(FILE_PATH)

    # Users
    ws_u = wb.add_worksheet("Users")
    headers_u = ["ID", "Full Name", "Email"]
    for col, h in enumerate(headers_u):
        ws_u.write(0, col, h)
    for row, u in enumerate(users["users"], start=1):
        ws_u.write(row, 0, u.get("id", ""))
        ws_u.write(row, 1, u.get("fullName", ""))
        ws_u.write(row, 2, u.get("email", ""))

    # Jobs
    headers_j = ["Name", "Type", "URL", "Description", "Is Buildable", "Is Folder", "Last Build", "Last Result", "Last Build Time"]
    ws_j = wb.add_worksheet("Jobs")
    for col, h in enumerate(headers_j):
        ws_j.write(0, col, h)
    for row, j in enumerate(jobs["jobs"], start=1):
        for col, key in enumerate(["name", "type", "url", "description", "isBuildable", "isFolder", "lastBuild", "lastResult", "lastBuildTime"]):
            ws_j.write(row, col, str(j.get(key, "")))

    # JobsWithBuilds
    ws_jb = wb.add_worksheet("JobsWithBuilds")
    for col, h in enumerate(headers_j):
        ws_jb.write(0, col, h)
    filtered_jobs = [j for j in jobs["jobs"] if j.get("lastBuild") not in (None, "", "null")]
    total_builds = sum(int(j.get("lastBuild", 0)) for j in filtered_jobs if str(j.get("lastBuild", "")).isdigit())
    for row, j in enumerate(filtered_jobs, start=1):
        for col, key in enumerate(["name", "type", "url", "description", "isBuildable", "isFolder", "lastBuild", "lastResult", "lastBuildTime"]):
            ws_jb.write(row, col, str(j.get(key, "")))

    # Nodes
    ws_n = wb.add_worksheet("Nodes")
    headers_n = ["Name", "Online", "Executors", "Labels", "Mode", "Description"]
    for col, h in enumerate(headers_n):
        ws_n.write(0, col, h)
    for row, n in enumerate(nodes["nodes"], start=1):
        for col, key in enumerate(["name", "online", "executors", "labels", "mode", "description"]):
            ws_n.write(row, col, str(n.get(key, "")))

    # AD_Group_Members
    ws_gm = wb.add_worksheet("AD_Group_Members")
    headers_gm = ["ad_group", "user", "displayName", "mail", "whenCreated", "user_dn"]
    for col, h in enumerate(headers_gm):
        ws_gm.write(0, col, h)
    for row, u in enumerate(ad_group_members, start=1):
        for col, key in enumerate(headers_gm):
            ws_gm.write(row, col, str(u.get(key, "")))

    # User_AD_Match
    ws_m = wb.add_worksheet("User_AD_Match")
    headers_m = ["jenkins_id", "fullName", "email", "ad_groups"]
    for col, h in enumerate(headers_m):
        ws_m.write(0, col, h)
    for row, m in enumerate(user_ad_match, start=1):
        for col, key in enumerate(headers_m):
            ws_m.write(row, col, str(m.get(key, "")))

    # Summary
    ws_s = wb.add_worksheet("Summary")
    ws_s.write(0, 0, "Дата")
    ws_s.write(1, 0, "Пользователи")
    ws_s.write(2, 0, "Джобы")
    ws_s.write(3, 0, "Джобы с билдами")
    ws_s.write(4, 0, "Всего билдов")
    ws_s.write(5, 0, "Ноды")
    ws_s.write(6, 0, "Пользователей AD")
    ws_s.write(7, 0, "Совпадений Jenkins↔AD")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws_s.write(0, 1, now)
    ws_s.write(1, 1, users["total"])
    ws_s.write(2, 1, jobs["total"])
    ws_s.write(3, 1, len(filtered_jobs))
    ws_s.write(4, 1, total_builds)
    ws_s.write(5, 1, nodes["total"])
    ws_s.write(6, 1, len(ad_group_members))
    ws_s.write(7, 1, len([m for m in user_ad_match if m["ad_groups"] != "NOT FOUND"]))

    wb.close()
    logger.info(f"✅ Excel отчёт успешно создан: {FILE_PATH}")


# MAIN
def main():
    logger.info("=== Старт инвентаризации Jenkins + AD-группы + сопоставление ===")
    try:
        users = get_users()
        jobs = get_jobs()
        nodes = get_nodes()
        ad_group_members = fetch_ldap_group_members()
        user_ad_match = match_jenkins_to_ad(users, ad_group_members)
        write_excel(users, jobs, nodes, ad_group_members, user_ad_match)
        logger.info("Инвентаризация завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
