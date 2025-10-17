import pandas as pd
import json
from datetime import timedelta

# === 1. Чтение логов ===
rows = []
with open("nexus.log", "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue

if not rows:
    raise SystemExit("Файл nexus.log пуст или не содержит корректных JSON-записей.")

df = pd.json_normalize(rows)

# === 2. Разбор полей и парсинг времени ===
df["timestamp"] = pd.to_datetime(
    df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f%z", errors="coerce"
)
mask = df["timestamp"].isna()
if mask.any():
    df.loc[mask, "timestamp"] = pd.to_datetime(df.loc[mask, "timestamp"], errors="coerce")

df = df.dropna(subset=["timestamp"])

# инициатор вида "anonymous/10.167.111.189" или "user1/10.0.0.2"
df[["username", "ip"]] = (
    df["initiator"].astype(str).str.extract(r"^(?:(.+?)/)?(\d+\.\d+\.\d+\.\d+)$")
)
df["username"] = df["username"].fillna("anonymous")

# фильтр: исключаем записи, где username содержит "*TASK"
df = df[~df["username"].str.contains(r"\*TASK", na=False)]

# имя репозитория
df["repo"] = df["attributes.repository.name"]

# === 3. Определение обращений (сессий) ===
df = df.sort_values(by=["ip", "repo", "timestamp"])
max_interval = timedelta(minutes=5)

df["prev_time"] = df.groupby(["ip", "repo"])["timestamp"].shift(1)
df["time_diff"] = df["timestamp"] - df["prev_time"]
df["new_session"] = (df["time_diff"] > max_interval) | df["time_diff"].isna()
df["session_id"] = df.groupby(["ip", "repo"])["new_session"].cumsum()

# === 4. Формируем таблицу сессий ===
sessions = (
    df.groupby(["ip", "repo", "session_id"])
    .agg(
        start_time=("timestamp", "min"),
        end_time=("timestamp", "max"),
        username=("username", lambda x: x.dropna().iloc[0] if len(x.dropna()) else "anonymous"),
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
print(repo_stats.to_string(index=False))

print("\n=== Уникальные пользователи ===")
print(f"Обычных пользователей: {len(users_normal)}")
print(f"Анонимных (уникальных IP): {sum(len(ips) for ips in users_anonymous['ip_list'])}")

print("\n=== Список обычных пользователей и их IP ===")
for _, row in users_normal.sort_values("username").iterrows():
    name = row["username"]
    ips = ", ".join(row["ip_list"])
    print(f"{name} ({ips})")

print("\n=== Список анонимусов (по одному IP на строку) ===")
for _, row in users_anonymous.iterrows():
    for ip in row["ip_list"]:
        print(f"{row['username']} ({ip})")
