import os
import re
import ssl
import time
import logging
import sqlite3
from pathlib import Path
from dotenv import load_dotenv
import hvac
import requests
import xlsxwriter
from ldap3 import Server, Connection, ALL, SUBTREE, Tls
from ldap3.utils.conv import escape_filter_chars

load_dotenv()

VAULT_ADDR = os.getenv("VAULT_ADDR")
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
CA_CERT = os.getenv("CA_CERT", "CA.crt")

AD_SERVER = os.getenv("AD_SERVER")
AD_USER = os.getenv("AD_USER")
AD_PASSWORD = os.getenv("AD_PASSWORD")
GROUP_SEARCH_BASE = os.getenv("AD_GROUP_SEARCH_BASE")
PEOPLE_SEARCH_BASE = os.getenv("AD_PEOPLE_SEARCH_BASE")
INCLUDE_NESTED = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("vault_ad_sync.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("vault_ad")

log.info("Запуск скрипта Vault + AD отчёт")

if not VAULT_ADDR or not VAULT_TOKEN:
    log.error("Не заданы VAULT_ADDR или VAULT_TOKEN")
    raise SystemExit(1)

if not GROUP_SEARCH_BASE or not PEOPLE_SEARCH_BASE:
    GROUP_SEARCH_BASE = "OU=URALSIB,DC=fc,DC=uralsibbank,DC=ru"
    PEOPLE_SEARCH_BASE = "DC=fc,DC=uralsibbank,DC=ru"

client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN, verify=CA_CERT)
if not client.is_authenticated():
    log.error("Не удалось аутентифицироваться в Vault")
    raise SystemExit(1)
log.info(f"Подключено к Vault: {VAULT_ADDR}")


def vault_request(method: str, path: str):
    method = method.upper()
    try:
        if method == "LIST":
            resp = client.list(path)
        elif method == "GET":
            resp = client.read(path)
        else:
            return {}
        if not resp:
            log.warning(f"Пустой ответ от Vault: {method} {path}")
            return {}
        return resp
    except Exception as e:
        log.error(f"Ошибка Vault {method} {path}: {e}")
        return {}


def get_vault_metrics(format="prometheus", use_api=True):
    session = requests.Session()
    url = f"{VAULT_ADDR}/v1/sys/metrics?format={format}" if use_api else f"{VAULT_ADDR}/metrics"
    try:
        r = session.get(url, verify=CA_CERT, timeout=15)
        r.raise_for_status()
        return r.text if format == "prometheus" else r.json()
    except Exception as e:
        log.error(f"Ошибка получения метрик: {e}")
        return None


def parse_kv_metrics(metrics_text: str):
    pattern = re.compile(r'vault[_\s]*secret[_\s]*kv[_\s]*count\s*\{[^}]*mount_point="([^"]+)"[^}]*\}\s+(\d+)', re.IGNORECASE)
    results = []
    total = 0
    for m in pattern.finditer(metrics_text):
        mount = m.group(1).rstrip("/")
        count = int(m.group(2))
        results.append({"mount_point": mount, "count": count})
        total += count
    log.info(f"KV-монтов: {len(results)}, секретов: {total}")
    return results, total


def get_aliases():
    resp = vault_request("LIST", "identity/alias/id")
    key_info = (resp.get("data") or {}).get("key_info", {})
    rows = []
    stats = {}
    for _, info in key_info.items():
        meta = info.get("metadata", {}) or {}
        mount_type = (info.get("mount_type") or "").lower().strip()
        effective_username = meta.get("effectiveUsername") or meta.get("service_account_name") or meta.get("name") or info.get("name")
        name_for_excel = effective_username if mount_type == "kubernetes" else info.get("name")
        rows.append({
            "name": name_for_excel,
            "effective_username": effective_username,
            "mount_type": mount_type,
            "namespace": meta.get("service_account_namespace", ""),
        })
        stats[mount_type] = stats.get(mount_type, 0) + 1
    stats_rows = [{"auth_type": k, "count": v} for k, v in stats.items()]
    log.info(f"Alias-ов: {len(rows)}")
    return rows, stats_rows


def get_token_stats():
    resp = vault_request("LIST", "auth/token/accessors")
    total = len((resp.get("data") or {}).get("keys", []))
    log.info(f"Токенов: {total}")
    return [{"active_tokens": total}]


def normalize_name(n: str) -> str:
    if not n:
        return ""
    n = n.strip().lower()
    if "@" in n:
        n = n.split("@")[0]
    return n.replace(".", "").replace("-", "")


def get_unique_users(alias_rows):
    filtered = [r for r in alias_rows if r["mount_type"] not in ("userpass", "approle")]
    uniq = {}
    for r in filtered:
        name = (r.get("effective_username") or r.get("name") or "").strip()
        if not name:
            continue
        key = normalize_name(name)
        if key not in uniq:
            uniq[key] = {"unique_user": name, "all_logins": set(), "namespaces": set()}
        uniq[key]["all_logins"].add(f"{r['mount_type']}:{r['name']}")
        if r.get("namespace"):
            uniq[key]["namespaces"].add(r["namespace"])
    res = [{
        "unique_user": u["unique_user"],
        "all_logins": ", ".join(sorted(u["all_logins"])),
        "namespaces": ", ".join(sorted(u["namespaces"])) if u["namespaces"] else "",
    } for u in uniq.values()]
    log.info(f"Уникальных пользователей: {len(res)}")
    return res


def paged_search(conn, **kwargs):
    rows = []
    for item in conn.extend.standard.paged_search(generator=True, paged_size=1000, **kwargs):
        if item.get("type") == "searchResEntry":
            rows.append(item)
    return rows


def get_ad_group_members(conn, group_name, include_nested=True):
    name_esc = escape_filter_chars(group_name)
    group_filter = f"(&(objectClass=group)(|(cn={name_esc})(sAMAccountName={name_esc})(name={name_esc})))"
    groups = paged_search(conn, search_base=GROUP_SEARCH_BASE, search_filter=group_filter, search_scope=SUBTREE,
                          attributes=["distinguishedName", "cn", "sAMAccountName", "whenCreated"])
    if not groups:
        return {"group": group_name, "found": False, "members": []}
    group_dn = groups[0]["attributes"]["distinguishedName"]
    group_dn_esc = escape_filter_chars(group_dn)
    member_clause = f"(memberOf:1.2.840.113556.1.4.1941:={group_dn_esc})" if include_nested else f"(memberOf={group_dn_esc})"
    user_filter = f"(&(objectClass=user)(!(objectClass=computer)){member_clause})"
    rows = paged_search(conn, search_base=PEOPLE_SEARCH_BASE, search_filter=user_filter, search_scope=SUBTREE,
                        attributes=["sAMAccountName", "displayName", "mail", "whenCreated", "distinguishedName"])
    members = []
    for r in rows:
        a = r["attributes"]
        members.append({
            "ad_group": group_name,
            "user": a.get("sAMAccountName", ""),
            "displayName": a.get("displayName", ""),
            "mail": (a.get("mail", "") or "").lower(),
            "user_dn": a.get("distinguishedName", ""),
            "user_created": str(a.get("whenCreated", "")),
        })
    return {"group": group_name, "found": True, "members": members}


def load_bk_users():
    log.info("Загружаем BK SQLite...")
    conn = sqlite3.connect("bk.sqlite")
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM bk").fetchall()
    conn.close()
    data = [dict(r) for r in rows]
    log.info(f"BK пользователей: {len(data)}")
    return data


def match_ad_to_bk(ad_users, bk_users):
    log.info("Сравнение AD пользователей с BK по Email...")
    bk_index = {}
    for r in bk_users:
        email = (r.get("Email") or "").strip().lower()
        if email:
            bk_index[email] = r
    matched = []
    for u in ad_users:
        email = (u.get("mail") or "").strip().lower()
        if not email:
            continue
        if email in bk_index:
            log.info(f"✔ Найден в BK: {email}")
            matched.append(bk_index[email])
        else:
            log.info(f"⚠ Не найден в BK (возможная тех. учетка): {email}")
    log.info(f"Совпадений: {len(matched)}")
    return matched


def write_excel(filename, aliases, tokens, alias_stats, unique_users, kv_stats, kv_total, ad_users, bk_matched):
    wb = xlsxwriter.Workbook(filename)

    def ws(name, data):
        w = wb.add_worksheet(name[:31])
        if not data:
            w.write(0, 0, "Нет данных")
            return
        headers = list(data[0].keys())
        for c, h in enumerate(headers):
            w.write(0, c, h)
        for r, row in enumerate(data, start=1):
            for c, h in enumerate(headers):
                w.write(r, c, str(row.get(h, "")))

    ws("Summary", [{
        "Vault": VAULT_ADDR,
        "Aliases": len(aliases),
        "Unique Users": len(unique_users),
        "Active Tokens": tokens[0]["active_tokens"],
        "KV Mounts": len(kv_stats),
        "Secrets Total": kv_total,
        "AD Users": len(ad_users),
        "BK Matched": len(bk_matched),
    }])

    ws("Aliases", aliases)
    ws("Unique Users", unique_users)
    ws("Alias Stats", alias_stats)
    ws("Token Stats", tokens)
    ws("KV Mounts", kv_stats)
    ws("AD Users", ad_users)
    ws("BK_Matched_Users", bk_matched)

    wb.close()
    log.info(f"Excel сохранён: {filename}")


def main():
    aliases, alias_stats = get_aliases()
    tokens = get_token_stats()
    unique_users = get_unique_users(aliases)

    m = get_vault_metrics(format="prometheus", use_api=True)
    if m:
        kv_stats, kv_total = parse_kv_metrics(m)
    else:
        kv_stats, kv_total = [], 0

    log.info("LDAP bind...")
    tls = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=CA_CERT)
    server = Server(AD_SERVER, use_ssl=True, get_info=ALL, tls=tls)
    conn = Connection(server, AD_USER, AD_PASSWORD, auto_bind=True)

    resp = vault_request("LIST", "auth/ldap/groups")
    groups = (resp.get("data") or {}).get("keys", [])

    ad_users = []
    total = len(groups)
    for idx, g in enumerate(groups, start=1):
        log.info(f"Группа {idx}/{total}: {g}")
        d = get_ad_group_members(conn, g, include_nested=INCLUDE_NESTED)
        if not d["found"]:
            log.warning(f"Группа не найдена: {g}")
            continue
        ad_users.extend(d["members"])

    conn.unbind()

    bk = load_bk_users()
    bk_matched = match_ad_to_bk(ad_users, bk)

    write_excel(
        "vault_usage_report.xlsx",
        aliases,
        tokens,
        alias_stats,
        unique_users,
        kv_stats,
        kv_total,
        ad_users,
        bk_matched,
    )


if __name__ == "__main__":
    main()
