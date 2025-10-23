import os
import re
from pathlib import Path
from dotenv import load_dotenv
import hvac
import xlsxwriter
import requests


# === Инициализация ===
load_dotenv()

VAULT_ADDR = os.getenv("VAULT_ADDR")
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
CA_CERT = os.getenv("CA_CERT", "CA.crt")

if not VAULT_ADDR or not VAULT_TOKEN:
    raise SystemExit("❌ Не заданы VAULT_ADDR или VAULT_TOKEN")

client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN, verify=CA_CERT)
if not client.is_authenticated():
    raise SystemExit("❌ Не удалось аутентифицироваться в Vault")

print(f"✅ Подключено к Vault: {VAULT_ADDR}")


# ============================================================
# 🔧 Универсальная функция Vault API (только GET/LIST)
# ============================================================
def vault_request(method: str, path: str):
    """Безопасный read-only запрос к Vault API (через hvac.Client)."""

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

        # hvac возвращает dict вида {"data": {...}}, уже готовый к использованию
        return resp

    except Exception as e:
        print(f"⚠️ Ошибка при запросе {method} {path}: {e}")
        return {}



# ============================================================
# 🧮  Метрики Vault (Prometheus)
# ============================================================
def get_vault_metrics(
    format: str = "prometheus", use_api: bool = True, include_token: bool = False
):
    """Возвращает метрики Vault."""
    session = requests.Session()
    headers = {}
    if include_token:
        headers["X-Vault-Token"] = VAULT_TOKEN

    if use_api:
        url = f"{VAULT_ADDR}/v1/sys/metrics?format={format}"
    else:
        url = f"{VAULT_ADDR}/metrics"

    try:
        r = session.get(url, headers=headers, verify=CA_CERT, timeout=15)
        r.raise_for_status()
        return r.text if format == "prometheus" else r.json()
    except Exception as e:
        print(f"⚠️ Ошибка при получении метрик с {url}: {e}")
        return None


# ============================================================
# 📊 Парсинг метрик vault_secret_kv_count
# ============================================================
def parse_kv_metrics(metrics_text: str):
    """
    Извлекает mount_point и count из строк формата:
    vault_secret_kv_count{cluster="...", mount_point="kv-xxx/", namespace="root"} 7
    """
    pattern = re.compile(
        r'vault[_\s]*secret[_\s]*kv[_\s]*count\s*\{[^}]*mount_point="([^"]+)"[^}]*\}\s+(\d+)',
        re.IGNORECASE,
    )

    results = []
    total_count = 0

    for match in pattern.finditer(metrics_text):
        mount_point = match.group(1)
        try:
            count = int(match.group(2))
        except ValueError:
            count = 0
        results.append({"mount_point": mount_point, "count": count})
        total_count += count

    print(f"📦 Найдено KV-монтов: {len(results)}, всего секретов: {total_count}")
    return results, total_count


# ============================================================
# 🔹 Алиасы пользователей
# ============================================================
def get_aliases():
    resp = vault_request("LIST", "identity/alias/id")
    key_info = (resp.get("data") or {}).get("key_info", {})
    if not key_info:
        print("⚠️ Alias-ов не найдено (key_info пуст).")
        return [], []

    rows = []
    stats = {}

    for aid, info in key_info.items():
        meta = info.get("metadata", {}) or {}
        mount_type = (info.get("mount_type") or "").lower().strip()
        effective_username = (
            meta.get("effectiveUsername")
            or meta.get("service_account_name")
            or meta.get("name")
            or info.get("name")
        )

        if mount_type == "kubernetes":
            name = effective_username
        else:
            name = info.get("name")

        row = {
            "name": name,
            "mount_type": mount_type,
            "namespace": meta.get("service_account_namespace", ""),
        }
        rows.append(row)
        stats[mount_type] = stats.get(mount_type, 0) + 1

    print(f"🔹 Найдено alias-ов: {len(rows)}")
    stats_rows = [{"auth_type": k, "count": v} for k, v in sorted(stats.items())]
    return rows, stats_rows


# ============================================================
# 🔸 LDAP-группы
# ============================================================
def get_ldap_groups():
    resp = vault_request("LIST", "auth/ldap/groups")
    groups = (resp.get("data") or {}).get("keys", [])
    print(f"🔸 LDAP-групп в Vault: {len(groups)}")
    return [{"ldap_group": g} for g in groups]


# ============================================================
# 🔑 Активные токены
# ============================================================
def get_token_stats():
    resp = vault_request("LIST", "auth/token/accessors")
    tokens = (resp.get("data") or {}).get("keys", [])
    total = len(tokens)
    print(f"🔑 Активных токенов: {total}")
    return [{"active_tokens": total}]


# ============================================================
# 👤 Уникальные пользователи
# ============================================================
def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = name.strip().lower()
    if "@" in name:
        name = name.split("@")[0]
    name = name.replace(".", "").replace("-", "")
    return name


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
                "has_ldap": (r["mount_type"] == "ldap"),
                "all_logins": set(),
                "namespaces": set(),
            }
        else:
            if r["mount_type"] == "ldap" and not unique[key]["has_ldap"]:
                unique[key]["unique_user"] = eff_name
                unique[key]["has_ldap"] = True

        login_info = f"{r['mount_type']}:{r['name']}"
        unique[key]["all_logins"].add(login_info)
        if r.get("namespace"):
            unique[key]["namespaces"].add(r["namespace"])

    result = []
    for u in unique.values():
        result.append(
            {
                "unique_user": u["unique_user"],
                "all_logins": ", ".join(sorted(u["all_logins"])),
                "namespaces": ", ".join(sorted(u["namespaces"]))
                if u["namespaces"]
                else "",
            }
        )

    print(f"👤 Уникальных пользователей: {len(result)}")
    return result


# ============================================================
# 📊 Формирование Excel-отчёта
# ============================================================
def write_excel(
    filename, aliases, groups, tokens, alias_stats, unique_users, kv_stats, kv_total
):
    out = Path(filename)
    workbook = xlsxwriter.Workbook(out)
    bold = workbook.add_format({"bold": True, "bg_color": "#F0F0F0"})

    def write_sheet(ws_name, data):
        ws = workbook.add_worksheet(ws_name)
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

    write_sheet("Aliases", aliases)
    write_sheet("Unique Users", unique_users)
    write_sheet("Auth Types Summary", alias_stats)
    write_sheet("LDAP Groups", groups)
    write_sheet("Token Stats", tokens)
    write_sheet("KV Mounts", kv_stats)

    summary = workbook.add_worksheet("Summary")
    summary.write("A1", "Vault Address", bold)
    summary.write("B1", VAULT_ADDR)
    summary.write("A2", "Всего алиасов")
    summary.write("B2", len(aliases))
    summary.write("A3", "Уникальных пользователей")
    summary.write("B3", len(unique_users))
    summary.write("A4", "LDAP групп")
    summary.write("B4", len(groups))
    summary.write("A5", "Активных токенов")
    summary.write("B5", tokens[0]["active_tokens"] if tokens else 0)
    summary.write("A6", "KV mount points")
    summary.write("B6", len(kv_stats))
    summary.write("A7", "Секретов всего")
    summary.write("B7", kv_total)

    workbook.close()
    print(f"\n📁 Отчёт готов: {out.resolve()}")


# ============================================================
# 🚀 Основной запуск
# ============================================================
def main():
    aliases, alias_stats = get_aliases()
    groups = get_ldap_groups()
    tokens = get_token_stats()
    unique_users = get_unique_users(aliases)

    print("\n📈 Получаем метрики Vault...")
    metrics_text = get_vault_metrics(format="prometheus", use_api=True)
    kv_stats, kv_total = ([], 0)
    if metrics_text:
        kv_stats, kv_total = parse_kv_metrics(metrics_text)

    write_excel(
        "vault_usage_report.xlsx",
        aliases,
        groups,
        tokens,
        alias_stats,
        unique_users,
        kv_stats,
        kv_total,
    )


if __name__ == "__main__":
    main()
