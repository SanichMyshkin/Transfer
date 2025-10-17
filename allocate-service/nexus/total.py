import pandas as pd
import json
from datetime import timedelta

LOG_FILE = "nexus.jsonl"
FALLBACK_INTERVAL = timedelta(seconds=15)


# === 1. Чтение логов (только нужные поля) ===
def parse_log_line(line: str):
    try:
        d = json.loads(line)
        attrs = d.get("attributes", {})
        return {
            "timestamp": d.get("timestamp"),
            "initiator": d.get("initiator", ""),
            "repo": attrs.get("repository.name"),
            "path": attrs.get("path") or attrs.get("name"),
            "action": attrs.get("action"),
            "status": attrs.get("status"),
            "request_id": attrs.get("request.id") or attrs.get("transaction.id"),
        }
    except json.JSONDecodeError:
        return None


def load_logs(filename):
    records = []
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            rec = parse_log_line(line.strip())
            if rec:
                records.append(rec)
    return pd.DataFrame(records)


df = load_logs(LOG_FILE)

if df.empty:
    raise SystemExit(f"Файл {LOG_FILE} пуст или не содержит корректных JSON-записей.")


# === 2. Разбор времени и инициатора ===
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp"])

df[["username", "ip"]] = (
    df["initiator"].astype(str).str.extract(r"^(?:(.+?)/)?(\d+\.\d+\.\d+\.\d+)$")
)
df["username"] = df["username"].fillna("anonymous")
df = df[~df["username"].str.contains(r"\*TASK", na=False)]

# === 3. Разделяем обращения с request_id и без ===
has_id = df["request_id"].notna()

# --- 3a. Обращения с request_id ---
reqs_with_id = (
    df[has_id]
    .groupby(["repo", "path", "action", "request_id"])
    .agg(
        start_time=("timestamp", "min"),
        end_time=("timestamp", "max"),
        username=("username", "first"),
        ip=("ip", "first"),
        status=("status", lambda x: x.dropna().iloc[-1] if len(x.dropna()) else None),
    )
    .reset_index()
)

# --- 3b. Обращения без request_id ---
df_noid = df[~has_id].copy()
df_noid = df_noid.sort_values(["repo", "path", "ip", "username", "timestamp"])

df_noid["prev_time"] = df_noid.groupby(["repo", "path", "ip", "username"])["timestamp"].shift(1)
df_noid["time_diff"] = df_noid["timestamp"] - df_noid["prev_time"]
df_noid["new_request"] = (df_noid["time_diff"] > FALLBACK_INTERVAL) | df_noid["time_diff"].isna()
df_noid["req_group"] = df_noid.groupby(["repo", "path", "ip", "username"])["new_request"].cumsum()

reqs_noid = (
    df_noid.groupby(["repo", "path", "ip", "username", "action", "req_group"])
    .agg(
        start_time=("timestamp", "min"),
        end_time=("timestamp", "max"),
        status=("status", lambda x: x.dropna().iloc[-1] if len(x.dropna()) else None),
    )
    .reset_index()
)

# Приводим к общей структуре
reqs_noid["request_id"] = None
reqs_noid = reqs_noid[
    ["repo", "path", "action", "request_id", "ip", "username", "start_time", "end_time", "status"]
]

reqs_with_id = reqs_with_id[
    ["repo", "path", "action", "request_id", "ip", "username", "start_time", "end_time", "status"]
]

# === 4. Объединяем все обращения ===
requests = pd.concat([reqs_with_id, reqs_noid], ignore_index=True)
requests["duration_sec"] = (requests["end_time"] - requests["start_time"]).dt.total_seconds()

# === 5. Фильтруем по успешным статусам (опционально) ===
requests = requests[(requests["status"].isna()) | (requests["status"].str.contains("SUCCESS|FINISH|OK", case=False, na=True))]

# === 6. Сводка по репозиториям ===
repo_stats = (
    requests.groupby("repo")
    .agg(
        total_requests=("request_id", "count"),
        total_users=("username", pd.Series.nunique),
    )
    .reset_index()
)

# === 7. Список IP для каждого пользователя ===
user_ips = (
    requests.groupby("username")["ip"]
    .unique()
    .apply(lambda arr: sorted(list(arr)))
    .reset_index()
    .rename(columns={"ip": "ip_list"})
)

# === 8. Разделяем обычных и анонимных ===
anon_names = {"anonymous", "*UNKNOWN"}
users_normal = user_ips[~user_ips["username"].isin(anon_names)].copy()
users_anonymous = user_ips[user_ips["username"].isin(anon_names)].copy()

# === 9. Вывод в консоль ===
print("=== Сводка по репозиториям ===")
if repo_stats.empty:
    print("Нет данных по репозиториям.")
else:
    print(repo_stats.to_string(index=False))

print("\n=== Уникальные пользователи ===")
print(f"Обычных пользователей: {len(users_normal)}")
print(f"Анонимных (уникальных IP): {sum(len(ips) for ips in users_anonymous['ip_list'])}")

print("\n=== Список обычных пользователей и их IP ===")
if users_normal.empty:
    print("Нет зарегистрированных пользователей.")
else:
    for _, row in users_normal.sort_values("username").iterrows():
        name = row["username"]
        ips = ", ".join(row["ip_list"])
        print(f"{name} ({ips})")

print("\n=== Список анонимусов (по одному IP на строку) ===")
if users_anonymous.empty:
    print("Нет анонимных обращений.")
else:
    for _, row in users_anonymous.iterrows():
        for ip in row["ip_list"]:
            print(f"{row['username']} ({ip})")
