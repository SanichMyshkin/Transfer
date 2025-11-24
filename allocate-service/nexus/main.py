import pandas as pd
import json
from datetime import timedelta
from log_loader import load_all_audit_logs


"""
Анализ логов Nexus: группировка обращений пользователей по репозиториям.

Механизм работы:
----------------
1. Логи сортируются по инициатору, репозиторию и времени.
2. Каждое обращение (session) — это последовательность запросов
   от одного инициатора к одному репозиторию без пауз дольше max_interval.
   Если пауза между соседними логами больше max_interval — начинается новое обращение.
3. После первичной группировки короткие подряд идущие обращения
   объединяются, если пауза между ними меньше merge_gap.

Параметры:
-----------
max_interval : timedelta
    Максимально допустимая пауза между логами внутри одного обращения.
merge_gap : timedelta
    Максимально допустимая пауза между соседними обращениями,
    чтобы они считались одной логической сессией пользователя.

Итог:
------
На выходе формируется Excel-файл с несколькими таблицами:
- Сводка по репозиториям
- Пользователи по каждому репозиторию
- Обычные пользователи (логины + IP)
- Анонимные пользователи (IP)
"""


def parse_log_record(d: dict):
    """Приведение сырой записи (dict) к твоему формату или пропуск"""
    initiator = d.get("initiator", "")

    # Пропускаем системные задания Nexus (*TASK, system, scheduled и т.п.)
    if isinstance(initiator, str) and "task" in initiator.lower():
        return None
    if d.get("domain") == "tasks" or d.get("type") == "scheduled":
        return None

    repo_name = d.get("attributes", {}).get("repository.name") or d.get(
        "attributes", {}
    ).get("repositoryName")

    return {
        "timestamp": d.get("timestamp"),
        "initiator": initiator,
        "repo": repo_name,
    }


# ============================================
# 2. Загрузка всех логов из архива
# ============================================

print("Загрузка логов из архива...")

# вместо "trash/nexus.jsonl" — указываешь архив
df_raw = load_all_audit_logs("path/to/big_archive.zip")

# Парсим все записи аналогично твоей функции parse_log_line
records = []
for raw in df_raw.to_dict(orient="records"):
    parsed = parse_log_record(raw)
    if parsed:
        records.append(parsed)

df = pd.DataFrame(records)
if df.empty:
    raise SystemExit("Не найдено ни одной валидной записи.")


# ============================================
# 3. Приведение временных меток
# ============================================

df["timestamp"] = pd.to_datetime(
    df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f%z", errors="coerce"
)
df = df.dropna(subset=["timestamp"])

# Разбивка initiator на username / ip
df[["username", "ip"]] = (
    df["initiator"].astype(str).str.extract(r"^(?:(.+?)/)?(\d+\.\d+\.\d+\.\d+)$")
)
df["username"] = df["username"].fillna("anonymous")
df = df[~df["username"].str.contains(r"\*TASK", na=False)]

df = df.sort_values(by=["initiator", "repo", "timestamp"])


# ============================================
# 4. Формирование первичных обращений
# ============================================

max_interval = timedelta(minutes=5)
df["gap"] = (
    (df["initiator"] != df["initiator"].shift())
    | (df["repo"] != df["repo"].shift())
    | ((df["timestamp"] - df["timestamp"].shift()) > max_interval)
)
df["session_id"] = df["gap"].cumsum()

sessions = (
    df.groupby(["initiator", "repo", "session_id"])
    .agg(
        start_time=("timestamp", "min"),
        end_time=("timestamp", "max"),
        username=("username", "first"),
        ip=("ip", "first"),
    )
    .reset_index()
)


# ============================================
# 5. Объединение коротких подряд сессий
# ============================================

sessions = sessions.sort_values(by=["initiator", "repo", "start_time"])
merge_gap = timedelta(minutes=1)

sessions["merge_gap"] = (
    (sessions["initiator"] != sessions["initiator"].shift())
    | (sessions["repo"] != sessions["repo"].shift())
    | ((sessions["start_time"] - sessions["end_time"].shift()) > merge_gap)
)

sessions["merged_session_id"] = sessions["merge_gap"].cumsum()

sessions = (
    sessions.groupby(["initiator", "repo", "merged_session_id"])
    .agg(
        start_time=("start_time", "min"),
        end_time=("end_time", "max"),
        username=("username", "first"),
        ip=("ip", "first"),
    )
    .reset_index()
)


# ============================================
# 6. Определение идентификатора пользователя
# ============================================

sessions["user_identity"] = sessions.apply(
    lambda r: (
        r["username"] if r["username"] not in {"anonymous", "*UNKNOWN"} else r["ip"]
    ),
    axis=1,
)


# ============================================
# 7. Сводка по репозиториям
# ============================================

repo_stats = (
    sessions.groupby("repo")
    .agg(
        total_requests=("merged_session_id", "count"),
        total_users=("user_identity", pd.Series.nunique),
    )
    .reset_index()
)


# ============================================
# 8. Пользователи по каждому репозиторию
# ============================================


def combine_users_with_ips(group):
    mapping = {}
    for _, row in group.iterrows():
        u = row["username"]
        ip = row["ip"]
        if pd.isna(ip) and u in {"anonymous", "*UNKNOWN"}:
            continue
        if u in {"anonymous", "*UNKNOWN"}:
            mapping[ip] = None
        else:
            mapping[u] = ip
    parts = []
    for user, ip in sorted([(u, i) for u, i in mapping.items() if u is not None]):
        if ip:
            parts.append(f"{user} ({ip})")
        else:
            parts.append(user)
    return ", ".join(parts)


repo_users = (
    sessions.groupby("repo", group_keys=False)[["username", "ip"]]
    .apply(combine_users_with_ips)
    .reset_index(name="users")
)


# ============================================
# 9. Пользователи и IP
# ============================================

user_ips = (
    sessions.groupby("username")["ip"]
    .apply(lambda x: sorted([ip for ip in set(x) if pd.notna(ip)]))
    .reset_index()
    .rename(columns={"ip": "ip_list"})
)

anon_names = {"anonymous", "*UNKNOWN"}
users_normal = user_ips[~user_ips["username"].isin(anon_names)]
users_anonymous = user_ips[user_ips["username"].isin(anon_names)]

anon_rows = []
for _, row in users_anonymous.iterrows():
    for ip in row["ip_list"]:
        anon_rows.append({"username": row["username"], "ip": ip})

users_anonymous_flat = pd.DataFrame(anon_rows)


# ============================================
# 10. Убираем временную зону для Excel
# ============================================

for col in ["start_time", "end_time"]:
    if hasattr(sessions[col].dtype, "tz"):
        sessions[col] = sessions[col].dt.tz_localize(None)


# ============================================
# 11. Экспорт в Excel
# ============================================

output_file = "nexus_report.xlsx"


def prepend_instruction(df, text_lines):
    """Добавляет строки-инструкции перед таблицей, без смещения столбцов"""
    blank_row = {col: None for col in df.columns}
    instruction_rows = []
    for line in text_lines:
        row = blank_row.copy()
        first_col = list(df.columns)[0]
        row[first_col] = line
        instruction_rows.append(row)
    return pd.concat([pd.DataFrame(instruction_rows), df], ignore_index=True)


with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    # --- 1. Сводка по репозиториям ---
    text_repo = [
        "Эта таблица показывает, сколько обращений было к каждому репозиторию.",
        "Поля: total_requests — количество обращений, total_users — уникальных пользователей.",
        "",
    ]
    df_repo = prepend_instruction(repo_stats, text_repo)
    df_repo.to_excel(writer, sheet_name="Сводка по репозиториям", index=False)

    # --- 2. Пользователи по каждому репозиторию ---
    text_repo_users = [
        "Здесь видно, кто именно обращался к каждому репозиторию.",
        "Формат: username (ip). Если только IP — пользователь анонимный.",
        "",
    ]
    df_repo_users = prepend_instruction(repo_users, text_repo_users)
    df_repo_users.to_excel(
        writer, sheet_name="Пользователи по репозиторию", index=False
    )

    # --- 3. Обычные пользователи ---
    text_normal = [
        "Список зарегистрированных пользователей и IP-адресов, с которых они подключались.",
        "",
    ]
    df_normal = prepend_instruction(users_normal, text_normal)
    df_normal.to_excel(writer, sheet_name="Обычные пользователи", index=False)

    # --- 4. Анонимные пользователи ---
    text_anon = ["Анонимные подключения без логина. Каждый IP — отдельная строка.", ""]
    df_anon = prepend_instruction(users_anonymous_flat, text_anon)
    df_anon.to_excel(writer, sheet_name="Анонимные пользователи", index=False)

    # --- Автоширина ---
    all_sheets = {
        "Сводка по репозиториям": df_repo,
        "Пользователи по репозиторию": df_repo_users,
        "Обычные пользователи": df_normal,
        "Анонимные пользователи": df_anon,
    }

    for sheet_name, df_tmp in all_sheets.items():
        worksheet = writer.sheets[sheet_name]
        for i, col in enumerate(df_tmp.columns):
            max_len = max(len(str(col)), df_tmp[col].astype(str).map(len).max()) + 2
            worksheet.set_column(i, i, max_len)


print(f"\nГотово: {output_file}")
