import os
import json
from pathlib import Path
from dotenv import load_dotenv
import hvac
import xlsxwriter

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
# 🔧 Универсальная функция (только GET/LIST)
# ============================================================
def vault_request(method: str, path: str):
    """Безопасный read-only запрос к Vault API."""
    if not path.startswith("/v1/"):
        path = f"/v1/{path.lstrip('/')}"
    method = method.upper()
    try:
        if method == "LIST":
            resp = client.adapter.request("LIST", path)
        elif method == "GET":
            resp = client.adapter.get(path)
        else:
            raise ValueError(f"Метод {method} не разрешён (только GET/LIST)")
        return resp.json() if hasattr(resp, "json") else resp
    except Exception as e:
        print(f"⚠️ Ошибка при запросе {method} {path}: {e}")
        return {}

# ============================================================
# 🔹 1. Алиасы пользователей (кто и через что заходит)
# ============================================================
def get_aliases():
    """Возвращает всех пользователей/сервисы и статистику по типам логина."""
    resp = vault_request("GET", "identity/alias/id")
    key_info = (resp.get("data") or {}).get("key_info", {})
    if not key_info:
        print("⚠️ Alias-ов не найдено (key_info пуст).")
        return [], []

    rows = []
    stats = {}

    for aid, info in key_info.items():
        meta = info.get("metadata", {}) or {}
        mount_type = (info.get("mount_type") or "").lower().strip()
        username = (
            meta.get("effectiveUsername")
            or meta.get("service_account_name")
            or meta.get("name")
            or info.get("name")
        )

        row = {
            "alias_id": aid,
            "canonical_id": info.get("canonical_id"),
            "name": info.get("name"),
            "mount_type": mount_type,
            "mount_path": info.get("mount_path"),
            "effective_username": username,
            "namespace": meta.get("service_account_namespace", "")
        }
        rows.append(row)
        stats[mount_type] = stats.get(mount_type, 0) + 1

    print(f"🔹 Найдено alias-ов: {len(rows)}")
    print("📊 Типы аутентификации:")
    for k, v in stats.items():
        print(f"   {k:<15} → {v}")

    stats_rows = [{"auth_type": k, "count": v} for k, v in sorted(stats.items())]
    return rows, stats_rows

# ============================================================
# 🔸 2. LDAP-группы
# ============================================================
def get_ldap_groups():
    resp = vault_request("LIST", "auth/ldap/groups")
    groups = (resp.get("data") or {}).get("keys", [])
    print(f"🔸 LDAP-групп в Vault: {len(groups)}")
    return [{"ldap_group": g} for g in groups]

# ============================================================
# 🗄 3. KV-монты
# ============================================================
def get_kv_mounts():
    mounts = vault_request("GET", "sys/mounts").get("data", {})
    result = []
    for mpath, meta in mounts.items():
        if meta.get("type") == "kv" and mpath.endswith("/"):
            v = (meta.get("options", {}) or {}).get("version")
            result.append({
                "mount": mpath,
                "engine": "kv v2" if v == "2" else "kv v1"
            })
    print(f"🗄 Найдено KV-монтов: {len(result)}")
    return result

# ============================================================
# 🔑 4. Активные токены
# ============================================================
def get_token_stats():
    resp = vault_request("LIST", "auth/token/accessors")
    tokens = (resp.get("data") or {}).get("keys", [])
    total = len(tokens)
    print(f"🔑 Активных токенов: {total}")
    return [{"active_tokens": total}]

# ============================================================
# 📊 5. Формирование Excel-отчёта
# ============================================================
def write_excel(filename, aliases, groups, kvs, tokens, alias_stats):
    out = Path(filename)
    workbook = xlsxwriter.Workbook(out)
    bold = workbook.add_format({"bold": True, "bg_color": "#F0F0F0"})

    def write_sheet(ws_name, data):
        ws = workbook.add_worksheet(ws_name)
        if not data:
            ws.write(0, 0, "Нет данных")
            return
        if not isinstance(data[0], dict):
            ws.write(0, 0, f"Ошибка: ожидались dict, получен {type(data[0])}")
            ws.write(1, 0, str(data))
            return
        headers = list(data[0].keys())
        for col, h in enumerate(headers):
            ws.write(0, col, h, bold)
        for row_idx, item in enumerate(data, start=1):
            for col, h in enumerate(headers):
                ws.write(row_idx, col, str(item.get(h, "")))

    write_sheet("Aliases", aliases)
    write_sheet("Auth Types Summary", alias_stats)
    write_sheet("LDAP Groups", groups)
    write_sheet("KV Mounts", kvs)
    write_sheet("Token Stats", tokens)

    summary = workbook.add_worksheet("Summary")
    summary.write("A1", "Vault Address", bold)
    summary.write("B1", VAULT_ADDR)
    summary.write("A2", "Всего пользователей")
    summary.write("B2", len(aliases))
    summary.write("A3", "LDAP групп")
    summary.write("B3", len(groups))
    summary.write("A4", "KV Mounts")
    summary.write("B4", len(kvs))
    summary.write("A5", "Активных токенов")
    summary.write("B5", tokens[0]["active_tokens"] if tokens else 0)

    workbook.close()
    print(f"\n📁 Отчёт готов: {out.resolve()}")

# ============================================================
# 🚀 Основной запуск
# ============================================================
def main():
    aliases, alias_stats = get_aliases()
    groups = get_ldap_groups()
    kvs = get_kv_mounts()
    tokens = get_token_stats()
    write_excel("vault_usage_report.xlsx", aliases, groups, kvs, tokens, alias_stats)

if __name__ == "__main__":
    main()
