import os
import json
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import hvac

# === Загрузка конфигурации ===
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

# === Утилита LIST ===
def vault_list(path):
    try:
        resp = client.adapter.request("LIST", path)
        data = resp if isinstance(resp, dict) else resp.json()
        return (data.get("data") or {}).get("keys", [])
    except Exception:
        try:
            resp = client.adapter.get(path, headers={"X-HTTP-Method-Override": "LIST"})
            if hasattr(resp, "status_code") and resp.status_code == 404:
                return []
            data = resp if isinstance(resp, dict) else resp.json()
            return (data.get("data") or {}).get("keys", [])
        except Exception:
            return []

# === 1️⃣ LDAP группы из Vault ===
ldap_groups = vault_list("/v1/auth/ldap/groups") or []
df_groups = pd.DataFrame({"Vault LDAP Groups": ldap_groups})
print(f"🔹 Найдено LDAP-групп в Vault: {len(ldap_groups)}")

# === 2️⃣ Получаем accessor всех auth backend'ов ===
auths = client.sys.list_auth_methods()["data"]
backend_map = {meta["accessor"]: path for path, meta in auths.items()}
ldap_accessor = next((a for a, p in backend_map.items() if p == "auth/ldap/"), None)

# === 3️⃣ Получаем пользователей (entities) ===
entities = vault_list("/v1/identity/entity/id") or []
entity_data = []

for eid in entities:
    try:
        resp = client.adapter.get(f"/v1/identity/entity/id/{eid}")
        e = resp.json().get("data", {})
        if not e:
            continue

        aliases = e.get("aliases", [])
        for a in aliases:
            login_backend = backend_map.get(a.get("mount_accessor"), "unknown")
            entity_data.append({
                "entity_id": eid,
                "name": e.get("name"),
                "email": e.get("metadata", {}).get("email", ""),
                "login_type": login_backend,
                "alias_name": a.get("name"),
                "policies": ", ".join(e.get("policies", [])),
            })
    except Exception:
        continue

df_users = pd.DataFrame(entity_data)
print(f"🔸 Реальных пользователей Vault: {len(df_users)}")

# === 4️⃣ Подсчёт токенов ===
tokens = vault_list("/v1/auth/token/accessors") or []
df_tokens = pd.DataFrame([{"Active Tokens": len(tokens)}])
print(f"🔑 Активных токенов: {len(tokens)}")

# === 5️⃣ Создаём Excel отчёт ===
output_file = Path("vault_usage_report.xlsx")
with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    df_groups.to_excel(writer, index=False, sheet_name="Vault LDAP Groups")
    df_users.to_excel(writer, index=False, sheet_name="Vault Users")
    df_tokens.to_excel(writer, index=False, sheet_name="Token Stats")

print(f"\n📊 Отчёт готов: {output_file.resolve()}")
