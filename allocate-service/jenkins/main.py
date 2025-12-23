import os
import sys
import ssl
import time
import logging
import urllib3
import xlsxwriter
import sqlite3
import re
from datetime import datetime
from dotenv import load_dotenv
from ldap3 import Server, Connection, ALL, SUBTREE, Tls
from ldap3.utils.conv import escape_filter_chars
from jenkins_groovy import JenkinsGroovyClient
from jenkins_scripts import SCRIPT_USERS, SCRIPT_JOBS, SCRIPT_NODES, SCRIPT_AD_GROUP

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

JENKINS_URL = os.getenv("JENKINS_URL")
USER = os.getenv("USER")
TOKEN = os.getenv("TOKEN")
FILE_PATH = os.path.join(os.getcwd(), "jenkins_inventory.xlsx")

AD_SERVER = os.getenv("AD_SERVER")
AD_USER = os.getenv("AD_USER")
AD_PASSWORD = os.getenv("AD_PASSWORD")
AD_BASE = os.getenv("AD_PEOPLE_SEARCH_BASE")
CA_CERT = os.getenv("CA_CERT", "CA.crt")

BK_SQLITE_PATH = os.getenv("BK_SQLITE_PATH")

client = JenkinsGroovyClient(JENKINS_URL, USER, TOKEN, is_https=False)


def get_users():
    data = client.run_script(SCRIPT_USERS)
    logger.info(f"Пользователей Jenkins: {data['total']}")
    return data


def get_jobs():
    data = client.run_script(SCRIPT_JOBS)
    return data


def get_nodes():
    data = client.run_script(SCRIPT_NODES)
    return data


def get_ad_groups():
    data = client.run_script(SCRIPT_AD_GROUP)
    return data.get("ad_groups", [])


def connect_ldap():
    tls = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=CA_CERT)
    server = Server(AD_SERVER, use_ssl=True, get_info=ALL, tls=tls)
    return Connection(server, AD_USER, AD_PASSWORD, auto_bind=True)


def safe_get(d, k):
    v = d.get(k, "")
    if isinstance(v, (list, tuple)):
        return v[0] if v else ""
    return v or ""


def get_users_from_ad_group(conn, group_name):
    name_esc = escape_filter_chars(group_name)
    flt = f"(&(objectClass=group)(|(cn={name_esc})(sAMAccountName={name_esc})(name={name_esc})))"
    conn.search(AD_BASE, flt, SUBTREE, attributes=["member"])
    if not conn.entries:
        return []
    members = conn.entries[0].member.values if "member" in conn.entries[0] else []
    users = []
    for dn in members:
        conn.search(
            dn,
            "(objectClass=user)",
            SUBTREE,
            attributes=["sAMAccountName", "displayName", "mail", "whenCreated"],
        )
        if conn.entries:
            a = conn.entries[0].entry_attributes_as_dict
            users.append(
                {
                    "ad_group": group_name,
                    "user": safe_get(a, "sAMAccountName"),
                    "displayName": safe_get(a, "displayName"),
                    "mail": safe_get(a, "mail").lower(),
                    "whenCreated": str(safe_get(a, "whenCreated")),
                    "user_dn": dn,
                }
            )
    return users


def fetch_ldap_group_members():
    groups = get_ad_groups()
    conn = connect_ldap()
    result = []
    for g in groups:
        result.extend(get_users_from_ad_group(conn, g))
    conn.unbind()
    return result


def match_jenkins_to_ad(jenkins_users, ad_members):
    ad_by_mail = {}
    ad_by_user = {}
    for a in ad_members:
        if a.get("mail"):
            ad_by_mail.setdefault(a["mail"], []).append(a["ad_group"])
        if a.get("user"):
            ad_by_user.setdefault(a["user"].lower(), []).append(a["ad_group"])
    out = []
    for u in jenkins_users["users"]:
        groups = set()
        if u.get("email", "").lower() in ad_by_mail:
            groups.update(ad_by_mail[u["email"].lower()])
        if u.get("id", "").lower() in ad_by_user:
            groups.update(ad_by_user[u["id"].lower()])
        out.append(
            {
                "jenkins_id": u.get("id", ""),
                "fullName": u.get("fullName", ""),
                "email": u.get("email", ""),
                "ad_groups": ", ".join(sorted(groups)) if groups else "NOT FOUND",
            }
        )
    return out


def load_bk_users():
    if not BK_SQLITE_PATH:
        return []
    conn = sqlite3.connect(BK_SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM bk").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def match_jenkins_with_bk(jenkins_users, bk_users):
    bk_by_login = {(u.get("sAMAccountName") or "").lower(): u for u in bk_users}
    bk_by_email = {(u.get("Email") or "").lower(): u for u in bk_users}
    matched = []
    unmatched = []
    for u in jenkins_users["users"]:
        login = (u.get("id") or "").lower()
        email = (u.get("email") or "").lower()
        found = bk_by_login.get(login) or bk_by_email.get(email)
        if found:
            matched.append(found)
        else:
            unmatched.append(u)
    return matched, unmatched


def split_unmatched(unmatched):
    tech = []
    fired = []

    def is_fio(name):
        p = name.split()
        return len(p) >= 2 and all(re.fullmatch(r"[А-Яа-яЁё]+", x) for x in p)

    for u in unmatched:
        name = (u.get("fullName") or "").strip()
        login = (u.get("id") or "").strip()
        if is_fio(name):
            fired.append(u)
        elif "." in login and login.lower() == login:
            fired.append(u)
        else:
            tech.append(u)
    return tech, fired


def write_excel(
    users, jobs, nodes, ad_members, ad_match, bk_matched, bk_tech, bk_fired
):
    wb = xlsxwriter.Workbook(FILE_PATH)

    ws = wb.add_worksheet("Users")
    ws.write_row(0, 0, ["ID", "Full Name", "Email"])
    for i, u in enumerate(users["users"], 1):
        ws.write_row(i, 0, [u.get("id", ""), u.get("fullName", ""), u.get("email", "")])

    ws = wb.add_worksheet("AD_Group_Members")
    ws.write_row(
        0, 0, ["ad_group", "user", "displayName", "mail", "whenCreated", "user_dn"]
    )
    for i, u in enumerate(ad_members, 1):
        ws.write_row(
            i,
            0,
            [
                u.get(k, "")
                for k in [
                    "ad_group",
                    "user",
                    "displayName",
                    "mail",
                    "whenCreated",
                    "user_dn",
                ]
            ],
        )

    ws = wb.add_worksheet("User_AD_Match")
    ws.write_row(0, 0, ["jenkins_id", "fullName", "email", "ad_groups"])
    for i, u in enumerate(ad_match, 1):
        ws.write_row(
            i,
            0,
            [u.get(k, "") for k in ["jenkins_id", "fullName", "email", "ad_groups"]],
        )

    ws = wb.add_worksheet("BK_Matched")
    if bk_matched:
        h = list(bk_matched[0].keys())
        ws.write_row(0, 0, h)
        for i, r in enumerate(bk_matched, 1):
            ws.write_row(i, 0, [r.get(x, "") for x in h])

    ws = wb.add_worksheet("BK_Tech")
    ws.write_row(0, 0, ["id", "fullName", "email"])
    for i, u in enumerate(bk_tech, 1):
        ws.write_row(i, 0, [u.get("id", ""), u.get("fullName", ""), u.get("email", "")])

    ws = wb.add_worksheet("BK_Fired")
    ws.write_row(0, 0, ["id", "fullName", "email"])
    for i, u in enumerate(bk_fired, 1):
        ws.write_row(i, 0, [u.get("id", ""), u.get("fullName", ""), u.get("email", "")])

    ws = wb.add_worksheet("Summary")
    ws.write_row(0, 0, ["Дата", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    ws.write_row(1, 0, ["Jenkins Users", users["total"]])
    ws.write_row(2, 0, ["BK Matched", len(bk_matched)])
    ws.write_row(3, 0, ["BK Tech", len(bk_tech)])
    ws.write_row(4, 0, ["BK Fired", len(bk_fired)])

    wb.close()
    logger.info(f"Excel создан: {FILE_PATH}")


def main():
    users = get_users()
    jobs = get_jobs()
    nodes = get_nodes()
    ad_members = fetch_ldap_group_members()
    ad_match = match_jenkins_to_ad(users, ad_members)
    bk_users = load_bk_users()
    bk_matched, bk_unmatched = match_jenkins_with_bk(users, bk_users)
    bk_tech, bk_fired = split_unmatched(bk_unmatched)
    write_excel(users, jobs, nodes, ad_members, ad_match, bk_matched, bk_tech, bk_fired)


if __name__ == "__main__":
    main()
