import os
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
# 🔧 Универсальная функция Vault API (только GET/LIST)
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
# 🔹 1. Алиасы пользователей
# ============================================================
def get_aliases():
    """Возвращает всех пользователей/сервисы и статистику по типу логина."""
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

        # ✅ Правильная логика формирования поля "name"
        if mount_type == "kubernetes":
            name = effective_username
        else:
            name = info.get("name")

        row = {
            "name": name,
            "mount_type": mount_type,
            "effective_username": effective_username,
            "namespace": meta.get("service_account_namespace", ""),
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
# 🗄 3. KV-хранилища
# ============================================================
def get_kv_mounts():
    mounts = vault_request("GET", "sys/mounts").get("data", {})
    result = []
    for mpath, meta in mounts.items():
        if meta.get("type") in ("kv", "kv-v2") and mpath.endswith("/"):
            result.append({"mount": mpath})
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
# 👤 5. Уникальные пользователи
# ============================================================
def normalize_name(name: str) -> str:
    """Простая нормализация имени: без домена, точек и в нижнем регистре."""
    if not name:
        return ""
    name = name.strip().lower()
    if "@" in name:
        name = name.split("@")[0]
    name = name.replace(".", "").replace("-", "")
    return name


def get_unique_users(alias_rows):
    """
    Группирует алиасы в уникальных пользователей.
    Исключает типы userpass и approle.
    Приоритет имени — LDAP.
    """
    filtered = [r for r in alias_rows if r["mount_type"] not in ("userpass", "approle")]

    unique = {}
    for r in filtered:
        eff_name = (r.get("effective_username") or r.get("name") or "").strip()
        if not eff_name:
            continue
        key = normalize_name(eff_name)

        # если нет в словаре — создаем новую запись
        if key not in unique:
            unique[key] = {
                "unique_user": eff_name,
                "has_ldap": (r["mount_type"] == "ldap"),
                "all_logins": set(),
                "namespaces": set(),
            }
        else:
            # если встретился LDAP, обновляем приоритетное имя
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
# 📊 6. Формирование Excel-отчёта
# ============================================================
def write_excel(filename, aliases, groups, kvs, tokens, alias_stats, unique_users):
    out = Path(filename)
    workbook = xlsxwriter.Workbook(out)
    bold = workbook.add_format({"bold": True, "bg_color": "#F0F0F0"})

    def write_sheet(ws_name, data):
        ws = workbook.add_worksheet(ws_name)
        if not data:
            ws.write(0, 0, "Нет данных")
            return

        if not isinstance(data, list):
            ws.write(0, 0, f"Ошибка: ожидался list, получен {type(data)}")
            return
        if not isinstance(data[0], dict):
            ws.write(0, 0, f"Ошибка: элементы не dict, а {type(data[0])}")
            ws.write(1, 0, str(data[0]))
            return

        headers = list(data[0].keys())
        for col, h in enumerate(headers):
            ws.write(0, col, h, bold)
        for row_idx, item in enumerate(data, start=1):
            for col, h in enumerate(headers):
                ws.write(row_idx, col, str(item.get(h, "")))
        ws.set_column(0, len(headers) - 1, 25)

    # Пишем все листы
    write_sheet("Aliases", aliases)
    write_sheet("Unique Users", unique_users)
    write_sheet("Auth Types Summary", alias_stats)
    write_sheet("LDAP Groups", groups)
    write_sheet("KV Mounts", kvs)
    write_sheet("Token Stats", tokens)

    # Сводка
    summary = workbook.add_worksheet("Summary")
    summary.write("A1", "Vault Address", bold)
    summary.write("B1", VAULT_ADDR)
    summary.write("A2", "Всего алиасов")
    summary.write("B2", len(aliases))
    summary.write("A3", "Уникальных пользователей")
    summary.write("B3", len(unique_users))
    summary.write("A4", "LDAP групп")
    summary.write("B4", len(groups))
    summary.write("A5", "KV Mounts")
    summary.write("B5", len(kvs))
    summary.write("A6", "Активных токенов")
    summary.write("B6", tokens[0]["active_tokens"] if tokens else 0)

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

    unique_users = get_unique_users(aliases)

    write_excel(
        "vault_usage_report.xlsx",
        aliases,
        groups,
        kvs,
        tokens,
        alias_stats,
        unique_users,
    )


if __name__ == "__main__":
    main()
