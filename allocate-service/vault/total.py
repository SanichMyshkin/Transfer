import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import hvac

# === Загрузка переменных окружения ===
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
# 🔧 Универсальная функция Vault API (только GET и LIST)
# ============================================================
def vault_request(method: str, path: str):
    """Выполняет GET или LIST-запрос к Vault и возвращает JSON."""
    method = method.upper()
    if not path.startswith("/v1/"):
        path = f"/v1/{path.lstrip('/')}"
    try:
        if method == "LIST":
            resp = client.adapter.request("LIST", path)
        elif method == "GET":
            resp = client.adapter.get(path)
        else:
            raise ValueError(f"Метод {method} не разрешён (только GET/LIST).")
        return resp.json() if hasattr(resp, "json") else resp
    except Exception as e:
        print(f"⚠️ Ошибка при запросе {method} {path}: {e}")
        return {}

# ============================================================
# 🔹 1. LDAP-группы, подключённые к Vault
# ============================================================
def get_vault_ldap_groups():
    auths = vault_request("GET", "sys/auth").get("data", {})
    ldap_path = next((p for p, meta in auths.items() if meta.get("type") == "ldap"), "auth/ldap/")
    resp = vault_request("LIST", f"{ldap_path}groups")
    groups = (resp.get("data") or {}).get("keys", [])
    print(f"🔹 LDAP-групп в Vault: {len(groups)} ({ldap_path})")
    return pd.DataFrame({"Vault LDAP Groups": groups}), ldap_path

# ============================================================
# 🔸 2. Алиасы пользователей (тип логина, backend)
# ============================================================
def get_vault_aliases():
    alias_ids = (vault_request("LIST", "identity/alias/id").get("data") or {}).get("keys", [])
    print(f"🔸 Найдено alias ID: {len(alias_ids)}")

    rows = []
    for aid in alias_ids:
        data = (vault_request("GET", f"identity/alias/id/{aid}").get("data") or {})
        meta = data.get("metadata", {}) or {}
        rows.append({
            "alias_id": data.get("id"),
            "name": data.get("name"),
            "mount_type": data.get("mount_type"),
            "mount_path": data.get("mount_path"),
            "entity_id": data.get("canonical_id"),
            "effective_username": meta.get("effectiveUsername")
                or meta.get("service_account_name")
                or meta.get("name"),
            "namespace": meta.get("service_account_namespace", ""),
        })
    df = pd.DataFrame(rows)
    print(f"✅ Получено алиасов: {len(df)}")

    # Сводка по типам авторизации
    summary = df["mount_type"].value_counts().reset_index()
    summary.columns = ["auth_type", "users_count"]
    return df, summary

# ============================================================
# 🔑 3. Подсчёт активных токенов
# ============================================================
def get_vault_token_stats():
    resp = vault_request("LIST", "auth/token/accessors")
    tokens = (resp.get("data") or {}).get("keys", [])
    total = len(tokens)
    print(f"🔑 Активных токенов: {total}")
    df = pd.DataFrame([{"Active Tokens": total}])
    return df

# ============================================================
# 📊 4. Сборка Excel-отчёта
# ============================================================
def create_excel_report(df_groups, df_aliases, df_summary, df_tokens):
    output = Path("vault_usage_report.xlsx")
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_groups.to_excel(writer, index=False, sheet_name="Vault LDAP Groups")
        df_aliases.to_excel(writer, index=False, sheet_name="Vault Users")
        df_summary.to_excel(writer, index=False, sheet_name="Auth Types Summary")
        df_tokens.to_excel(writer, index=False, sheet_name="Token Stats")
    print(f"\n📁 Отчёт готов: {output.resolve()}")

# ============================================================
# 🚀 Основной запуск
# ============================================================
def main():
    df_groups, ldap_path = get_vault_ldap_groups()
    df_aliases, df_summary = get_vault_aliases()
    df_tokens = get_vault_token_stats()
    create_excel_report(df_groups, df_aliases, df_summary, df_tokens)

if __name__ == "__main__":
    main()
