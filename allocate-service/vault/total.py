import os
import re
import ssl
import json
from pathlib import Path
from dotenv import load_dotenv
import hvac
import xlsxwriter
import requests
from ldap3 import Server, Connection, ALL, SUBTREE, Tls
from ldap3.utils.conv import escape_filter_chars


# === Инициализация окружения ===
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

if not VAULT_ADDR or not VAULT_TOKEN:
    raise SystemExit("❌ Не заданы VAULT_ADDR или VAULT_TOKEN")

client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN, verify=CA_CERT)
if not client.is_authenticated():
    raise SystemExit("❌ Не удалось аутентифицироваться в Vault")

print(f"✅ Подключено к Vault: {VAULT_ADDR}")


# ============================================================
# 🔧 Vault API
# ============================================================
def vault_request(method: str, path: str):
    method = method.upper()
    try:
        if method == "LIST":
            resp = client.list(path)
        elif method == "GET":
            resp = client.read(path)
        else:
            raise ValueError(f"Метод {method} не разрешён (только GET/LIST)")
        if not resp:
            print(f"⚠️ Пустой ответ от Vault на {method} {path}")
            return {}
        return resp
    except Exception as e:
        print(f"⚠️ Ошибка при запросе {method} {path}: {e}")
        return {}


# ============================================================
# 🧮 Метрики Vault
# ============================================================
def get_vault_metrics(format="prometheus", use_api=True, include_token=False):
    session = requests.Session()
    headers = {}
    if include_token:
        headers["X-Vault-Token"] = VAULT_TOKEN

    url = (
        f"{VAULT_ADDR}/v1/sys/metrics?format={format}"
        if use_api
        else f"{VAULT_ADDR}/metrics"
    )
    try:
        r = session.get(url, headers=headers, verify=CA_CERT, timeout=15)
        r.raise_for_status()
        return r.text if format == "prometheus" else r.json()
    except Exception as e:
        print(f"⚠️ Ошибка при получении метрик с {url}: {e}")
        return None


def parse_kv_metrics(metrics_text: str):
    pattern = re.compile(
        r'vault[_\s]*secret[_\s]*kv[_\s]*count\s*\{[^}]*mount_point="([^"]+)"[^}]*\}\s+(\d+)',
        re.IGNORECASE,
    )
    results, total_count = [], 0
    for match in pattern.finditer(metrics_text):
        mount_point = match.group(1).rstrip("/")
        try:
            count = int(match.group(2))
        except ValueError:
            count = 0
        results.append({"mount_point": mount_point, "count": count})
        total_count += count
    print(f"📦 Найдено KV-монтов: {len(results)}, всего секретов: {total_count}")
    return results, total_count


# ============================================================
# 🔹 Алиасы, группы и токены
# ============================================================
def get_aliases():
    resp = vault_request("LIST", "identity/alias/id")
    key_info = (resp.get("data") or {}).get("key_info", {})
    if not key_info:
        print("⚠️ Alias-ов не найдено.")
        return [], []
    rows, stats = [], {}
    for aid, info in key_info.items():
        meta = info.get("metadata", {}) or {}
        mount_type = (info.get("mount_type") or "").lower().strip()
        effective_username = (
            meta.get("effectiveUsername")
            or meta.get("service_account_name")
            or meta.get("name")
            or info.get("name")
        )
        row = {
            "name": info.get("name"),
            "effective_username": effective_username,
            "mount_type": mount_type,
            "namespace": meta.get("service_account_namespace", ""),
        }
        rows.append(row)
        stats[mount_type] = stats.get(mount_type, 0) + 1
    print(f"🔹 Найдено alias-ов: {len(rows)}")
    stats_rows = [{"auth_type": k, "count": v} for k, v in sorted(stats.items())]
    return rows, stats_rows


def get_ldap_groups():
    resp = vault_request("LIST", "auth/ldap/groups")
    groups = (resp.get("data") or {}).get("keys", [])
    print(f"🔸 LDAP-групп в Vault: {len(groups)}")
    return [{"ldap_group": g} for g in groups]


def get_token_stats():
    resp = vault_request("LIST", "auth/token/accessors")
    tokens = (resp.get("data") or {}).get("keys", [])
    total = len(tokens)
    print(f"🔑 Активных токенов: {total}")
    return total


# ============================================================
# 👤 Уникальные пользователи
# ============================================================
def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = name.strip().lower()
    if "@" in name:
        name = name.split("@")[0]
    return name.replace(".", "").replace("-", "")


def get_unique_users(alias_rows):
    filtered = [r for r in alias_rows if r["mount_type"] not in ("userpass", "approle")]
    unique = {}
    for r in filtered:
        eff_name = (r.get("effective_username") or r.get("name") or "").strip()
        if not eff_name:
            continue
        key = normalize_name(eff_name)
        if key not in unique:
            unique[key] = {
                "unique_user": eff_name,
                "all_logins": set(),
                "namespaces": set(),
            }
        unique[key]["all_logins"].add(f"{r['mount_type']}:{r['name']}")
        if r.get("namespace"):
            unique[key]["namespaces"].add(r["namespace"])
    result = [
        {
            "unique_user": u["unique_user"],
            "all_logins": ", ".join(sorted(u["all_logins"])),
            "namespaces": ", ".join(sorted(u["namespaces"])) if u["namespaces"] else "",
        }
        for u in unique.values()
    ]
    print(f"👤 Уникальных пользователей: {len(result)}")
    return result


# ============================================================
# 🌐 AD: подключение и поиск пользователей групп
# ============================================================
def paged_search(conn, **kwargs):
    entries = []
    for item in conn.extend.standard.paged_search(
        generator=True, paged_size=1000, **kwargs
    ):
        if item.get("type") == "searchResEntry":
            entries.append(item)
    return entries


def get_ad_group_members(conn, group_name, include_nested=True):
    name_esc = escape_filter_chars(group_name)
    group_filter = f"(&(objectClass=group)(|(cn={name_esc})(sAMAccountName={name_esc})(name={name_esc})))"
    groups = paged_search(
        conn,
        search_base=GROUP_SEARCH_BASE,
        search_filter=group_filter,
        search_scope=SUBTREE,
        attributes=["distinguishedName", "cn", "sAMAccountName", "whenCreated"],
    )
    if not groups:
        return {"group": group_name, "members": [], "found": False}
    group_dn = groups[0]["attributes"]["distinguishedName"]
    group_created = str(groups[0]["attributes"].get("whenCreated", ""))
    group_dn_esc = escape_filter_chars(group_dn)
    member_clause = (
        f"(memberOf:1.2.840.113556.1.4.1941:={group_dn_esc})"
        if include_nested
        else f"(memberOf={group_dn_esc})"
    )
    user_filter = f"(&(objectClass=user)(!(objectClass=computer)){member_clause})"
    users = paged_search(
        conn,
        search_base=PEOPLE_SEARCH_BASE,
        search_filter=user_filter,
        search_scope=SUBTREE,
        attributes=[
            "sAMAccountName",
            "displayName",
            "mail",
            "whenCreated",
            "distinguishedName",
        ],
    )
    members = []
    for u in users:
        a = u["attributes"]
        members.append(
            {
                "user": a.get("sAMAccountName", ""),
                "displayName": a.get("displayName", ""),
                "mail": a.get("mail", ""),
                "user_dn": a.get("distinguishedName", ""),
                "user_created": str(a.get("whenCreated", "")),
            }
        )
    return {
        "group": group_name,
        "group_dn": group_dn,
        "group_created": group_created,
        "found": True,
        "members": members,
    }


# ============================================================
# 📊 Excel отчёт
# ============================================================
def write_excel(
    filename,
    aliases,
    groups,
    token_count,
    alias_stats,
    unique_users,
    kv_stats,
    kv_total,
    ad_compare,
):
    out = Path(filename)
    workbook = xlsxwriter.Workbook(out)
    bold = workbook.add_format({"bold": True, "bg_color": "#F0F0F0"})

    def write_sheet(ws_name, data):
        ws = workbook.add_worksheet(ws_name[:31])  # Excel limit
        if not data:
            ws.write(0, 0, "Нет данных")
            return
        headers = list(data[0].keys())
        for col, h in enumerate(headers):
            ws.write(0, col, h, bold)
        for row_idx, item in enumerate(data, start=1):
            for col, h in enumerate(headers):
                ws.write(row_idx, col, str(item.get(h, "")))
        ws.set_column(0, len(headers) - 1, 30)

    kv_team = []
    for kv in kv_stats:
        mount = kv["mount_point"].rstrip("/")
        match = re.match(r"^kv-([a-z0-9_-]+)$", mount, re.IGNORECASE)
        if match:
            kv_team.append({"team_kv": match.group(1)})

    write_sheet("Aliases", aliases)
    write_sheet("Unique Users", unique_users)
    write_sheet("Auth Types", alias_stats)
    write_sheet("LDAP Groups", groups)
    write_sheet("KV Mounts", kv_stats)
    write_sheet("Team KV", kv_team)
    write_sheet("AD vs Vault Users", ad_compare)

    summary = workbook.add_worksheet("Summary")
    summary.write("A1", "Vault Address", bold)
    summary.write("B1", VAULT_ADDR)
    summary.write("A2", "Алиасов")
    summary.write("B2", len(aliases))
    summary.write("A3", "Уникальных пользователей")
    summary.write("B3", len(unique_users))
    summary.write("A4", "LDAP групп")
    summary.write("B4", len(groups))
    summary.write("A5", "Активных токенов")
    summary.write("B5", token_count)
    summary.write("A6", "KV mount points")
    summary.write("B6", len(kv_stats))
    summary.write("A7", "Секретов всего")
    summary.write("B7", kv_total)
    summary.write("A8", "Командных KV")
    summary.write("B8", len(kv_team))
    summary.write("A9", "AD пользователей")
    summary.write("B9", len(ad_compare))

    workbook.close()
    print(f"\n📁 Отчёт готов: {out.resolve()}")


# ============================================================
# 🚀 Основной запуск
# ============================================================
def main():
    aliases, alias_stats = get_aliases()
    groups = get_ldap_groups()
    token_count = get_token_stats()
    unique_users = get_unique_users(aliases)

    print("\n📈 Получаем метрики Vault...")
    metrics_text = get_vault_metrics(format="prometheus", use_api=True)
    kv_stats, kv_total = ([], 0)
    if metrics_text:
        kv_stats, kv_total = parse_kv_metrics(metrics_text)

    print("\n🔍 Подключаемся к Active Directory...")
    tls = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=CA_CERT)
    server = Server(AD_SERVER, use_ssl=True, get_info=ALL, tls=tls)
    conn = Connection(server, AD_USER, AD_PASSWORD, auto_bind=True)

    print("📘 Сопоставляем AD ↔ Vault пользователей...")
    vault_usernames = {normalize_name(u["unique_user"]) for u in unique_users}
    ad_compare = []
    for g in groups:
        group_name = g["ldap_group"]
        group_data = get_ad_group_members(
            conn, group_name, include_nested=INCLUDE_NESTED
        )
        if not group_data["found"]:
            print(f"⚠️ Группа {group_name} не найдена в AD")
            continue
        for m in group_data["members"]:
            user_key = normalize_name(m["user"])
            ad_compare.append(
                {
                    "vault_group": group_name,
                    "ad_user": m["user"],
                    "displayName": m["displayName"],
                    "mail": m["mail"],
                    "in_vault": "✅" if user_key in vault_usernames else "❌",
                }
            )

    conn.unbind()

    write_excel(
        "vault_usage_report.xlsx",
        aliases,
        groups,
        token_count,
        alias_stats,
        unique_users,
        kv_stats,
        kv_total,
        ad_compare,
    )


if __name__ == "__main__":
    main()
