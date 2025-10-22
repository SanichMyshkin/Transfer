import os
import ssl
import json
import csv
from pathlib import Path
from ldap3 import Server, Connection, ALL, SUBTREE, Tls
from ldap3.utils.conv import escape_filter_chars
from dotenv import load_dotenv
import hvac

# === Загрузка конфигурации ===
load_dotenv()

VAULT_ADDR = os.getenv("VAULT_ADDR")
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
CA_CERT = os.getenv("CA_CERT", "CA.crt")
AD_SERVER = os.getenv("AD_SERVER")
AD_USER = os.getenv("AD_USER")
AD_PASSWORD = os.getenv("AD_PASSWORD")

GROUP_SEARCH_BASE = os.getenv("GROUP_SEARCH_BASE")
PEOPLE_SEARCH_BASE = os.getenv("PEOPLE_SEARCH_BASE")
INCLUDE_NESTED = os.getenv("INCLUDE_NESTED", "true").lower() == "true"

if not (VAULT_ADDR and VAULT_TOKEN and AD_SERVER and AD_USER and AD_PASSWORD):
    raise SystemExit("❌ Не заданы обязательные переменные окружения")

# === Подключение к Vault ===
client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN, verify=CA_CERT)
if not client.is_authenticated():
    raise SystemExit("❌ Не удалось аутентифицироваться в Vault")
print(f"✅ Подключено к Vault: {VAULT_ADDR}")

# === Подключение к AD ===
tls = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=CA_CERT)
server = Server(AD_SERVER, use_ssl=True, get_info=ALL, tls=tls)
conn = Connection(server, AD_USER, AD_PASSWORD, auto_bind=True)
print(f"✅ Подключено к AD: {AD_SERVER}")

# === Утилиты ===
def vault_list(path):
    """LIST-запрос с fallback'ом"""
    try:
        resp = client.adapter.request("LIST", path)
        data = resp if isinstance(resp, dict) else resp.json()
        return (data.get("data") or {}).get("keys", [])
    except Exception:
        try:
            resp = client.adapter.get(path, headers={"X-HTTP-Method-Override": "LIST"})
            if hasattr(resp, "status_code") and resp.status_code == 404:
                return None
            data = resp if isinstance(resp, dict) else resp.json()
            return (data.get("data") or {}).get("keys", [])
        except Exception:
            return None

def paged_search(conn, **kwargs):
    entries = []
    for item in conn.extend.standard.paged_search(generator=True, paged_size=1000, **kwargs):
        if item.get("type") == "searchResEntry":
            entries.append(item)
    return entries

# === 1. Vault LDAP-группы и пользователи из AD ===
vault_groups = vault_list("/v1/auth/ldap/groups") or []
print(f"🔹 Найдено групп в Vault: {len(vault_groups)}")

report = []
for group_name in vault_groups:
    name_esc = escape_filter_chars(group_name)
    group_filter = f"(&(objectClass=group)(|(cn={name_esc})(sAMAccountName={name_esc})(name={name_esc})))"
    groups = paged_search(conn, search_base=GROUP_SEARCH_BASE, search_filter=group_filter,
                          search_scope=SUBTREE, attributes=["distinguishedName", "cn", "whenCreated"])

    if not groups:
        print(f"⚠️ Группа {group_name} не найдена в AD")
        continue

    group_dn = groups[0]["attributes"]["distinguishedName"]
    group_created = str(groups[0]["attributes"].get("whenCreated", ""))

    member_clause = (
        f"(memberOf:1.2.840.113556.1.4.1941:={escape_filter_chars(group_dn)})"
        if INCLUDE_NESTED else f"(memberOf={escape_filter_chars(group_dn)})"
    )
    user_filter = f"(&(objectClass=user)(!(objectClass=computer)){member_clause})"
    users = paged_search(conn, search_base=PEOPLE_SEARCH_BASE, search_filter=user_filter,
                         search_scope=SUBTREE,
                         attributes=["sAMAccountName", "displayName", "mail", "whenCreated", "distinguishedName"])

    for u in users:
        a = u["attributes"]
        report.append({
            "vault_group": group_name,
            "ad_group_dn": group_dn,
            "group_created": group_created,
            "user": a.get("sAMAccountName", ""),
            "displayName": a.get("displayName", ""),
            "mail": a.get("mail", ""),
            "user_dn": a.get("distinguishedName", ""),
            "user_created": str(a.get("whenCreated", "")),
        })

# === 2. Подсчёт KV secrets ===
kv_report = []
mounts = client.sys.list_mounted_secrets_engines().get("data", {})
for mpath, meta in mounts.items():
    if meta.get("type") != "kv" or not mpath.endswith("/"):
        continue
    is_v2 = (meta.get("options", {}) or {}).get("version") == "2"

    def _count(prefix=""):
        list_path = f"/v1/{mpath}{'metadata/' if is_v2 else ''}{prefix}"
        keys = vault_list(list_path)
        if not keys:
            return 0
        total = 0
        for k in keys:
            if k.endswith("/"):
                total += _count(prefix + k)
            else:
                total += 1
        return total

    try:
        count = _count("")
        kv_report.append({"mount": mpath, "engine": "kv v2" if is_v2 else "kv v1", "secrets_count": count})
        print(f"🗝 {mpath} ({'v2' if is_v2 else 'v1'}): {count}")
    except Exception as e:
        kv_report.append({"mount": mpath, "engine": "kv v2" if is_v2 else "kv v1", "error": str(e)})

# === 3. Подсчёт токенов ===
token_count = 0
try:
    tokens = vault_list("/v1/auth/token/accessors") or []
    token_count = len(tokens)
    print(f"🔑 Всего токенов: {token_count}")
except Exception as e:
    print(f"⚠️ Ошибка при получении списка токенов: {e}")

# === 4. Сохраняем всё ===
Path("vault_full_report.json").write_text(json.dumps({
    "vault_groups": report,
    "kv_stats": kv_report,
    "token_count": token_count,
}, indent=2, ensure_ascii=False))

print("\n✅ Отчёт готов: vault_full_report.json")
conn.unbind()
