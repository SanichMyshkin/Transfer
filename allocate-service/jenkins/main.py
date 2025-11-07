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
from jenkins_scripts import SCRIPT_USERS, SCRIPT_JOBS, SCRIPT_NODES

# === Настройка логирования ===
LOG_FILE = os.path.join(os.getcwd(), "jenkins_inventory.log")

for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
)

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# === Jenkins credentials ===
JENKINS_URL = os.getenv("JENKINS_URL")
USER = os.getenv("USER")
TOKEN = os.getenv("TOKEN")
FILE_PATH = os.path.join(os.getcwd(), "jenkins_inventory.xlsx")

# === LDAP credentials ===
AD_SERVER = os.getenv("AD_SERVER")
AD_USER = os.getenv("AD_USER")
AD_PASSWORD = os.getenv("AD_PASSWORD")
AD_BASE = os.getenv("AD_PEOPLE_SEARCH_BASE")
CA_CERT = os.getenv("CA_CERT", "CA.crt")

client = JenkinsGroovyClient(JENKINS_URL, USER, TOKEN, is_https=False)


# ============================================================
# === Jenkins data fetchers ===
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


# ============================================================
# === LDAP functions ===
# ============================================================


def connect_ldap():
    logger.info(f"Подключаемся к LDAP: {AD_SERVER}")
    tls = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=CA_CERT)
    server = Server(AD_SERVER, use_ssl=True, get_info=ALL, tls=tls)
    return Connection(server, AD_USER, AD_PASSWORD, auto_bind=True)


def get_user_groups(conn, sam_or_mail):
    """Возвращает список групп AD по SAM или mail"""
    if not sam_or_mail:
        return None
    filt = escape_filter_chars(sam_or_mail)
    search = f"(|(sAMAccountName={filt})(mail={filt}))"
    conn.search(
        search_base=AD_BASE,
        search_filter=f"(&(objectClass=user){search})",
        search_scope=SUBTREE,
        attributes=["sAMAccountName", "displayName", "mail", "memberOf"],
    )
    if not conn.entries:
        return None
    entry = conn.entries[0]
    groups = entry.memberOf.values if "memberOf" in entry else []
    return [str(g.split(",")[0].replace("CN=", "")) for g in groups]


def map_jenkins_to_ldap(jenkins_users):
    """Сопоставляет пользователей Jenkins с LDAP-группами"""
    conn = connect_ldap()
    matched, unmatched, all_groups = [], [], set()

    for u in jenkins_users["users"]:
        uid = u.get("id")
        mail = u.get("email", "")
        groups = get_user_groups(conn, uid) or get_user_groups(conn, mail)
        if groups:
            matched.append(
                {
                    "jenkins_id": uid,
                    "fullName": u.get("fullName", ""),
                    "email": mail,
                    "ad_groups": ", ".join(groups),
                }
            )
            all_groups.update(groups)
        else:
            unmatched.append(
                {"jenkins_id": uid, "fullName": u.get("fullName", ""), "email": mail}
            )

    conn.unbind()
    logger.info(
        f"LDAP сопоставлено: {len(matched)}, не найдено: {len(unmatched)}, уникальных групп: {len(all_groups)}"
    )
    return matched, unmatched, len(all_groups)


# ============================================================
# === Excel Writer ===
# ============================================================


def write_excel(users, jobs, nodes, matched, unmatched, ad_group_count):
    """Перезаписывает Excel-файл полностью"""
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
        "Name",
        "Type",
        "URL",
        "Description",
        "Is Buildable",
        "Is Folder",
        "Last Build",
        "Last Result",
        "Last Build Time",
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

    # --- Jobs with Builds ---
    ws_jb = wb.add_worksheet("JobsWithBuilds")
    for col, h in enumerate(headers_j):
        ws_jb.write(0, col, h)

    filtered_jobs = [
        j for j in jobs["jobs"] if j.get("lastBuild") not in (None, "", "null")
    ]

    total_builds = 0
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
        try:
            total_builds += int(j.get("lastBuild", 0))
        except ValueError:
            pass

    logger.info(
        f"Добавлен лист JobsWithBuilds: {len(filtered_jobs)} записей, всего билдов: {total_builds}"
    )

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

    # --- Jenkins ↔ AD Mapping ---
    ws_m = wb.add_worksheet("Jenkins-AD")
    headers_m = ["Jenkins ID", "Full Name", "Email", "AD Groups"]
    for col, h in enumerate(headers_m):
        ws_m.write(0, col, h)
    for row, item in enumerate(matched, start=1):
        ws_m.write(row, 0, item["jenkins_id"])
        ws_m.write(row, 1, item["fullName"])
        ws_m.write(row, 2, item["email"])
        ws_m.write(row, 3, item["ad_groups"])

    start = len(matched) + 2
    ws_m.write(start, 0, "Непривязанные Jenkins учётки:")
    for row, item in enumerate(unmatched, start=start + 1):
        ws_m.write(row, 0, item["jenkins_id"])
        ws_m.write(row, 1, item["fullName"])
        ws_m.write(row, 2, item["email"])

    # --- Summary ---
    ws_s = wb.add_worksheet("Summary")
    ws_s.write(0, 0, "Дата")
    ws_s.write(1, 0, "Пользователи")
    ws_s.write(2, 0, "Джобы")
    ws_s.write(3, 0, "Джобы с билдами")
    ws_s.write(4, 0, "Всего билдов")
    ws_s.write(5, 0, "Ноды")
    ws_s.write(6, 0, "AD групп (уникальных)")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws_s.write(0, 1, now)
    ws_s.write(1, 1, users["total"])
    ws_s.write(2, 1, jobs["total"])
    ws_s.write(3, 1, len(filtered_jobs))
    ws_s.write(4, 1, total_builds)
    ws_s.write(5, 1, nodes["total"])
    ws_s.write(6, 1, ad_group_count)

    wb.close()
    logger.info(f"Excel полностью перезаписан: {FILE_PATH}")


# ============================================================
# === Основной поток ===
# ============================================================


def main():
    logger.info("=== Старт инвентаризации Jenkins ===")
    try:
        users = get_users()
        jobs = get_jobs()
        nodes = get_nodes()
        matched, unmatched, ad_group_count = map_jenkins_to_ldap(users)
        write_excel(users, jobs, nodes, matched, unmatched, ad_group_count)
        logger.info("Инвентаризация завершена успешно.")
    except Exception as e:
        logger.exception(f"Ошибка при инвентаризации: {e}")


if __name__ == "__main__":
    main()
