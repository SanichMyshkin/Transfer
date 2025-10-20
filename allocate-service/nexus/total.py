import pandas as pd
import json
from datetime import timedelta


# === 1. Чтение логов (только нужные поля) ===
def parse_log_line(line: str):
    try:
        d = json.loads(line)
        return {
            "timestamp": d.get("timestamp"),
            "initiator": d.get("initiator", ""),
            "repo": d.get("attributes", {}).get("repository.name"),
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


df = load_logs("trash/nexus.jsonl")

if df.empty:
    raise SystemExit(
        "Файл trash/nexus.jsonl пуст или не содержит корректных JSON-записей."
    )


# === 2. Парсим время и инициатора ===
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp"])

df[["username", "ip"]] = (
    df["initiator"].astype(str).str.extract(r"^(?:(.+?)/)?(\d+\.\d+\.\d+\.\d+)$")
)
df["username"] = df["username"].fillna("anonymous")

# убираем системные задачи
df = df[~df["username"].str.contains(r"\*TASK", na=False)]

# === 3. Сортировка и формирование сессий ===
df = df.sort_values(by=["ip", "repo", "timestamp"])
max_interval = timedelta(minutes=5)

# вместо groupby используем векторную логику (в 3–5 раз быстрее)
df["gap"] = (
    (df["ip"] != df["ip"].shift())
    | (df["repo"] != df["repo"].shift())
    | ((df["timestamp"] - df["timestamp"].shift()) > max_interval)
)
df["session_id"] = df["gap"].cumsum()

# === 4. Формируем таблицу сессий ===
sessions = (
    df.groupby(["ip", "repo", "session_id"])
    .agg(
        start_time=("timestamp", "min"),
        end_time=("timestamp", "max"),
        username=("username", "first"),
    )
    .reset_index()
)

# === 5. Список IP для каждого пользователя ===
user_ips = (
    sessions.groupby("username")["ip"]
    .unique()
    .apply(lambda arr: sorted(list(arr)))
    .reset_index()
    .rename(columns={"ip": "ip_list"})
)

# === 6. Статистика по репозиториям ===
repo_stats = (
    sessions.groupby("repo")
    .agg(
        total_requests=("session_id", "count"),
        total_users=("username", pd.Series.nunique),
    )
    .reset_index()
)

# === 7. Разделяем обычных и анонимных ===
anon_names = {"anonymous", "*UNKNOWN"}
users_normal = user_ips[~user_ips["username"].isin(anon_names)].copy()
users_anonymous = user_ips[user_ips["username"].isin(anon_names)].copy()

# === 8. Вывод в консоль ===
print("=== Сводка по репозиториям ===")
if repo_stats.empty:
    print("Нет данных по репозиториям.")
else:
    print(repo_stats.to_string(index=False))

print("\n=== Уникальные пользователи ===")
print(f"Обычных пользователей: {len(users_normal)}")
print(
    f"Анонимных (уникальных IP): {sum(len(ips) for ips in users_anonymous['ip_list'])}"
)

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
