import os
import sys
import ssl
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
# === Jenkins data ===
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
    """Получаем AD-группы из Jenkins"""
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


def get_all_ldap_users(conn, search_base=AD_BASE):
    """Получает всех пользователей из LDAP"""
    logger.info("Получаем всех пользователей LDAP...")
    conn.search(
        search_base=search_base,
        search_filter="(&(objectClass=user)(!(objectClass=computer)))",
        search_scope=SUBTREE,
        attributes=["sAMAccountName", "displayName", "mail", "distinguishedName", "whenCreated"],
    )
    results = []
    for e in conn.entries:
        a = e.entry_attributes_as_dict
        results.append({
            "sAMAccountName": a.get("sAMAccountName", [""])[0],
            "displayName": a.get("displayName", [""])[0],
            "mail": a.get("mail", [""])[0],
            "dn": a.get("distinguishedName", [""])[0],
            "whenCreated": str(a.get("whenCreated", [""])[0])
        })
    logger.info(f"Всего пользователей в LDAP: {len(results)}")
    return results


def get_users_from_ad_group(conn, group_name):
    """Возвращает пользователей конкретной AD-группы"""
    name_esc = escape_filter_chars(group_name)
    group_filter = f"(&(objectClass=group)(|(cn={name_esc})(sAMAccountName={name_esc})(name={name_esc})))"
    conn.search(
        search_base=AD_BASE,
        search_filter=group_filter,
        search_scope=SUBTREE,
        attributes=["distinguishedName", "cn", "member"],
    )
    if not conn.entries:
        return {"group": group_name, "found": False, "members": []}

    entry = conn.entries[0]
    members = entry.member.values if "member" in entry else []
    users = []

    for dn in members:
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
                "user": a.get("sAMAccountName", [""])[0],
                "displayName": a.get("displayName", [""])[0],
                "mail": a.get("mail", [""])[0],
                "whenCreated": str(a.get("whenCreated", [""])[0]),
                "user_dn": dn,
            })

    return {"group": group_name, "found": True, "members": users}


def fetch_ldap_data():
    """Получает все LDAP-пользователи и членов всех AD-групп из Jenkins"""
    groups = get_ad_groups()
    conn = connect_ldap()

    all_ldap_users = get_all_ldap_users(conn)
    ad_group_members = []

    for idx, group in enumerate(groups, start=1):
        logger.info(f"[{idx}/{len(groups)}] Обработка группы: {group}")
        try:
            g_data = get_users_from_ad_group(conn, group)
            if g_data["found"]:
                ad_group_members.extend(g_data["members"])
                logger.info(f"Группа {group}: найдено {len(g_data['members'])} пользователей")
            else:
                logger.warning(f"Группа {group} не найдена в AD")
        except Exception as e:
            logger.error(f"Ошибка при обработке {group}: {e}")

    conn.unbind()
    logger.info(f"Всего пользователей из всех групп: {len(ad_group_members)}")
    return all_ldap_users, ad_group_members


# ============================================================
# === Excel Writer ===
# ============================================================

def write_excel_with_ldap(users, jobs, nodes, ldap_users, ad_group_members):
    wb = xlsxwriter.Workbook(FILE_PATH)

    # --- Users ---
    ws_u = wb.add_worksheet("Users")
    headers_u = ["ID", "Full Name", "Email"]
    for col, h in enumerate(headers_u):
        ws_u.write(0, col, h)
    for row, u in enumerate(users["users"], start=1):
        ws_u.write(row, 0, u.get("id", ""))
        ws_u.write(row, 1, u.get("fullName", ""))
        ws_u.write(row, 2, u.get("email", ""))

    # --- Jobs ---
    ws_j = wb.add_worksheet("Jobs")
    headers_j = [
        "Name", "Type", "URL", "Description",
        "Is Buildable", "Is Folder", "Last Build",
        "Last Result", "Last Build Time"
    ]
    for col, h in enumerate(headers_j):
        ws_j.write(0, col, h)
    for row, j in enumerate(jobs["jobs"], start=1):
        ws_j.write(row, 0, j.get("name", ""))
        ws_j.write(row, 1, j.get("type", ""))
        ws_j.write(row, 2, j.get("url", ""))
        ws_j.write(row, 3, j.get("description", ""))
        ws_j.write(row, 4, str(j.get("isBuildable", "")))
        ws_j.write(row, 5, str(j.get("isFolder", "")))
        ws_j.write(row, 6, str(j.get("lastBuild", "")))
        ws_j.write(row, 7, str(j.get("lastResult", "")))
        ws_j.write(row, 8, str(j.get("lastBuildTime", "")))

    # --- JobsWithBuilds ---
    ws_jb = wb.add_worksheet("JobsWithBuilds")
    for col, h in enumerate(headers_j):
        ws_jb.write(0, col, h)

    filtered_jobs = [j for j in jobs["jobs"] if j.get("lastBuild") not in (None, "", "null")]
    total_builds = sum(int(j.get("lastBuild", 0)) for j in filtered_jobs if str(j.get("lastBuild", "")).isdigit())

    for row, j in enumerate(filtered_jobs, start=1):
        ws_jb.write(row, 0, j.get("name", ""))
        ws_jb.write(row, 1, j.get("type", ""))
        ws_jb.write(row, 2, j.get("url", ""))
        ws_jb.write(row, 3, j.get("description", ""))
        ws_jb.write(row, 4, str(j.get("isBuildable", "")))
        ws_jb.write(row, 5, str(j.get("isFolder", "")))
        ws_jb.write(row, 6, str(j.get("lastBuild", "")))
        ws_jb.write(row, 7, str(j.get("lastResult", "")))
        ws_jb.write(row, 8, str(j.get("lastBuildTime", "")))

    # --- Nodes ---
    ws_n = wb.add_worksheet("Nodes")
    headers_n = ["Name", "Online", "Executors", "Labels", "Mode", "Description"]
    for col, h in enumerate(headers_n):
        ws_n.write(0, col, h)
    for row, n in enumerate(nodes["nodes"], start=1):
        ws_n.write(row, 0, n.get("name", ""))
        ws_n.write(row, 1, str(n.get("online", "")))
        ws_n.write(row, 2, str(n.get("executors", "")))
        ws_n.write(row, 3, n.get("labels", ""))
        ws_n.write(row, 4, n.get("mode", ""))
        ws_n.write(row, 5, n.get("description", ""))

    # --- LDAP Users ---
    ws_lu = wb.add_worksheet("LDAP_Users")
    headers_lu = ["sAMAccountName", "displayName", "mail", "dn", "whenCreated"]
    for col, h in enumerate(headers_lu):
        ws_lu.write(0, col, h)
    for row, u in enumerate(ldap_users, start=1):
        ws_lu.write(row, 0, u.get("sAMAccountName", ""))
        ws_lu.write(row, 1, u.get("displayName", ""))
        ws_lu.write(row, 2, u.get("mail", ""))
        ws_lu.write(row, 3, u.get("dn", ""))
        ws_lu.write(row, 4, u.get("whenCreated", ""))

    # --- AD Group Members ---
    ws_gm = wb.add_worksheet("AD_Group_Members")
    headers_gm = ["ad_group", "user", "displayName", "mail", "whenCreated", "user_dn"]
    for col, h in enumerate(headers_gm):
        ws_gm.write(0, col, h)
    for row, u in enumerate(ad_group_members, start=1):
        ws_gm.write(row, 0, u.get("ad_group", ""))
        ws_gm.write(row, 1, u.get("user", ""))
        ws_gm.write(row, 2, u.get("displayName", ""))
        ws_gm.write(row, 3, u.get("mail", ""))
        ws_gm.write(row, 4, u.get("whenCreated", ""))
        ws_gm.write(row, 5, u.get("user_dn", ""))

    # --- Summary ---
    ws_s = wb.add_worksheet("Summary")
    ws_s.write(0, 0, "Дата")
    ws_s.write(1, 0, "Пользователи")
    ws_s.write(2, 0, "Джобы")
    ws_s.write(3, 0, "Джобы с билдами")
    ws_s.write(4, 0, "Всего билдов")
    ws_s.write(5, 0, "Ноды")
    ws_s.write(6, 0, "LDAP пользователей")
    ws_s.write(7, 0, "AD групп (Jenkins)")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws_s.write(0, 1, now)
    ws_s.write(1, 1, users["total"])
    ws_s.write(2, 1, jobs["total"])
    ws_s.write(3, 1, len(filtered_jobs))
    ws_s.write(4, 1, total_builds)
    ws_s.write(5, 1, nodes["total"])
    ws_s.write(6, 1, len(ldap_users))
    ws_s.write(7, 1, len(set([m["ad_group"] for m in ad_group_members])))

    wb.close()
    logger.info(f"Excel отчёт успешно создан: {FILE_PATH}")


# ============================================================
# === MAIN ===
# ============================================================

def main():
    logger.info("=== Старт инвентаризации Jenkins + LDAP ===")
    try:
        users = get_users()
        jobs = get_jobs()
        nodes = get_nodes()
        ldap_users, ad_group_members = fetch_ldap_data()
        write_excel_with_ldap(users, jobs, nodes, ldap_users, ad_group_members)
        logger.info("Инвентаризация завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
