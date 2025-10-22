import os
import json
import xlsxwriter
from pathlib import Path
from dotenv import load_dotenv
import hvac

# === Конфигурация ===
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
# 🔧 Универсальный read-only запрос
# ============================================================
def vault_request(method: str, path: str):
    """Выполняет GET или LIST-запрос к Vault."""
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
# 🔹 1. Алиасы (все пользователи / сервисы)
# ============================================================
def get_aliases():
    data = vault_request("LIST", "identity/alias/id")
    aliases = (data.get("data") or {}).get("keys", [])
    print(f"🔹 Найдено записей alias: {len(aliases)}")
    return [{"alias_id": a} for a in aliases]

# ============================================================
# 🔸 2. LDAP-группы (команды)
# ============================================================
def get_ldap_groups():
    data = vault_request("LIST", "auth/ldap/groups")
    groups = (data.get("data") or {}).get("keys", [])
    print(f"🔸 LDAP-групп в Vault: {len(groups)}")
    return [{"ldap_group": g} for g in groups]

# ============================================================
# 🔑 3. KV хранилища
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
# 🔐 4. Активные токены
# ============================================================
def get_token_stats():
    data = vault_request("LIST", "auth/token/accessors")
    tokens = (data.get("data") or {}).get("keys", [])
    total = len(tokens)
    print(f"🔑 Активных токенов: {total}")
    return [{"active_tokens": total}]

# ============================================================
# 📊 5. Формирование Excel
# ============================================================
def write_excel(filename, aliases, groups, kvs, tokens):
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

    write_sheet("Aliases", aliases)
    write_sheet("LDAP Groups", groups)
    write_sheet("KV Mounts", kvs)
    write_sheet("Token Stats", tokens)

    # Сводка
    summary = workbook.add_worksheet("Summary")
    summary.write("A1", "Vault Address", bold)
    summary.write("B1", VAULT_ADDR)
    summary.write("A2", "Всего Aliases")
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
    aliases = get_aliases()
    groups = get_ldap_groups()
    kvs = get_kv_mounts()
    tokens = get_token_stats()
    write_excel("vault_usage_report.xlsx", aliases, groups, kvs, tokens)

if __name__ == "__main__":
    main()
