import os
import re
import ssl
import time
import logging
from pathlib import Path
from dotenv import load_dotenv
import hvac
import requests
import xlsxwriter
from ldap3 import Server, Connection, ALL, SUBTREE, Tls
from ldap3.utils.conv import escape_filter_chars


# ============================================================
# ⚙️ Настройки и инициализация
# ============================================================
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

# === Логирование ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("vault_ad_sync.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("vault_ad")

log.info("🚀 Запуск скрипта Vault + AD отчёт")

if not VAULT_ADDR or not VAULT_TOKEN:
    log.error("❌ Не заданы VAULT_ADDR или VAULT_TOKEN")
    raise SystemExit(1)

if not GROUP_SEARCH_BASE or not PEOPLE_SEARCH_BASE:
    log.warning("⚠️ Не заданы AD_*_SEARCH_BASE, применяю дефолтные значения.")
    GROUP_SEARCH_BASE = "OU=URALSIB,DC=fc,DC=uralsibbank,DC=ru"
    PEOPLE_SEARCH_BASE = "DC=fc,DC=uralsibbank,DC=ru"

client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN, verify=CA_CERT)
if not client.is_authenticated():
    log.error("❌ Не удалось аутентифицироваться в Vault")
    raise SystemExit(1)
log.info(f"✅ Подключено к Vault: {VAULT_ADDR}")


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
            log.warning(f"⚠️ Пустой ответ от Vault на {method} {path}")
            return {}
        return resp
    except Exception as e:
        log.error(f"⚠️ Ошибка при запросе {method} {path}: {e}")
        return {}


# ============================================================
# 🧮 Метрики Vault
# ============================================================
def get_vault_metrics(format="prometheus", use_api=True):
    session = requests.Session()
    url = (
        f"{VAULT_ADDR}/v1/sys/metrics?format={format}"
        if use_api
        else f"{VAULT_ADDR}/metrics"
    )
    try:
        r = session.get(url, verify=CA_CERT, timeout=15)
        r.raise_for_status()
        return r.text if format == "prometheus" else r.json()
    except Exception as e:
        log.error(f"⚠️ Ошибка при получении метрик: {e}")
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
    log.info(f"📦 Найдено KV-монтов: {len(results)}, всего секретов: {total_count}")
    return results, total_count


# ============================================================
# 🔹 Алиасы, токены и аутентификация
# ============================================================
def get_aliases():
    resp = vault_request("LIST", "identity/alias/id")
    key_info = (resp.get("data") or {}).get("key_info", {})
    if not key_info:
        log.warning("⚠️ Alias-ов не найдено.")
        return [], []
    rows, stats = {}, {}
    rows = []
    stats = {}

    for aid, info in key_info.items():
        meta = info.get("metadata", {}) or {}
        mount_type = (info.get("mount_type") or "").lower().strip()

        # вычисляем "эффективное имя" (важно для Kubernetes)
        effective_username = (
            meta.get("effectiveUsername")
            or meta.get("service_account_name")
            or meta.get("name")
            or info.get("name")
        )

        # имя, которое попадёт в таблицу — зависит от типа аутентификации
        if mount_type == "kubernetes":
            name_for_excel = effective_username
        else:
            name_for_excel = info.get("name")

        row = {
            "name": name_for_excel,
            "effective_username": effective_username,  # внутреннее поле
            "mount_type": mount_type,
            "namespace": meta.get("service_account_namespace", ""),
        }
        rows.append(row)
        stats[mount_type] = stats.get(mount_type, 0) + 1

    log.info(f"🔹 Найдено alias-ов: {len(rows)}")
    stats_rows = [{"auth_type": k, "count": v} for k, v in sorted(stats.items())]
    return rows, stats_rows


def get_token_stats():
    resp = vault_request("LIST", "auth/token/accessors")
    tokens = (resp.get("data") or {}).get("keys", [])
    total = len(tokens)
    log.info(f"🔑 Активных токенов: {total}")
    return [{"active_tokens": total}]


# ============================================================
# 👤 Уникальные пользователи (Vault)
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
    log.info(f"👤 Уникальных пользователей: {len(result)}")
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
    """Возвращает пользователей конкретной AD-группы"""
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
                "ad_group": group_name,
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
    tokens,
    alias_stats,
    unique_users,
    kv_stats,
    kv_total,
    ad_full_list,
):
    out = Path(filename)
    workbook = xlsxwriter.Workbook(out)
    bold = workbook.add_format({"bold": True, "bg_color": "#F0F0F0"})

    def write_sheet(ws_name, data, exclude_fields=None):
        exclude_fields = exclude_fields or []
        ws = workbook.add_worksheet(ws_name[:31])
        if not data:
            ws.write(0, 0, "Нет данных")
            return
        headers = [h for h in data[0].keys() if h not in exclude_fields]
        for col, h in enumerate(headers):
            ws.write(0, col, h, bold)
        for row_idx, item in enumerate(data, start=1):
            for col, h in enumerate(headers):
                ws.write(row_idx, col, str(item.get(h, "")))
        ws.set_column(0, len(headers) - 1, 30)

    # 🧾 Листы
    write_sheet(
        "Aliases", aliases, exclude_fields=["effective_username"]
    )  # скрываем колонку
    write_sheet("Unique Users", unique_users)
    write_sheet("Auth Types Summary", alias_stats)
    write_sheet("Token Stats", tokens)
    write_sheet("KV Mounts", kv_stats)
    write_sheet("AD Group Members", ad_full_list)

    # Team KV (kv-name-code)
    kv_team = []
    for kv in kv_stats:
        mount = kv["mount_point"].rstrip("/")
        match = re.match(r"^kv-([a-z0-9]+-\d+)$", mount, re.IGNORECASE)
        if match:
            kv_team.append({"team_kv": match.group(1)})
    write_sheet("Team KV", kv_team)

    # Summary
    summary = workbook.add_worksheet("Summary")
    summary.write("A1", "Vault Address", bold)
    summary.write("B1", VAULT_ADDR)
    summary.write("A2", "Алиасов")
    summary.write("B2", len(aliases))
    summary.write("A3", "Уникальных пользователей")
    summary.write("B3", len(unique_users))
    summary.write("A4", "Активных токенов")
    summary.write("B4", tokens[0]["active_tokens"] if tokens else 0)
    summary.write("A5", "KV mount points")
    summary.write("B5", len(kv_stats))
    summary.write("A6", "Секретов всего")
    summary.write("B6", kv_total)
    summary.write("A7", "Командных KV")
    summary.write("B7", len(kv_team))
    summary.write("A8", "AD пользователей (всего)")
    summary.write("B8", len(ad_full_list))

    workbook.close()
    log.info(f"📁 Отчёт готов: {out.resolve()}")


# ============================================================
# 🚀 Основной запуск
# ============================================================
def main():
    aliases, alias_stats = get_aliases()
    tokens = get_token_stats()
    unique_users = get_unique_users(aliases)

    log.info("📈 Получаем метрики Vault...")
    metrics_text = get_vault_metrics(format="prometheus", use_api=True)
    kv_stats, kv_total = ([], 0)
    if metrics_text:
        kv_stats, kv_total = parse_kv_metrics(metrics_text)

    log.info("🔍 Подключаемся к Active Directory...")
    tls = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=CA_CERT)
    server = Server(AD_SERVER, use_ssl=True, get_info=ALL, tls=tls)
    conn = Connection(server, AD_USER, AD_PASSWORD, auto_bind=True)

    # Получаем список групп из Vault API
    groups_resp = vault_request("LIST", "auth/ldap/groups")
    groups = (groups_resp.get("data") or {}).get("keys", [])

    log.info("📘 Получаем пользователей из AD-групп...")
    ad_full_list = []
    start_all = time.perf_counter()
    total_groups = len(groups)

    for idx, group_name in enumerate(groups, start=1):
        start = time.perf_counter()
        log.info(f"[{idx}/{total_groups}] ▶️ Обработка группы: {group_name}")

        try:
            group_data = get_ad_group_members(
                conn, group_name, include_nested=INCLUDE_NESTED
            )
        except Exception as e:
            log.error(f"❌ Ошибка при запросе группы {group_name}: {e}")
            continue

        if not group_data["found"]:
            log.warning(f"⚠️ Группа {group_name} не найдена в AD.")
            continue

        members = group_data["members"]
        ad_full_list.extend(members)
        elapsed = time.perf_counter() - start
        log.info(
            f"✅ Группа {group_name}: найдено {len(members)} пользователей, время {elapsed:.2f} сек.\n"
        )

    conn.unbind()
    total_time = time.perf_counter() - start_all
    log.info(
        f"🎯 Всего пользователей из всех AD-групп: {len(ad_full_list)}. Время: {total_time:.1f} сек."
    )

    write_excel(
        "vault_usage_report.xlsx",
        aliases,
        tokens,
        alias_stats,
        unique_users,
        kv_stats,
        kv_total,
        ad_full_list,
    )


if __name__ == "__main__":
    main()
