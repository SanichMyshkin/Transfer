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

df = pd.json_normalize(rows)

# === 2. Разбор полей и парсинг времени (с попыткой задать формат, чтобы избежать ворнинга) ===
# ожидаемый формат: 2025-10-15 07:13:45,060+0000
df["timestamp"] = pd.to_datetime(
    df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f%z", errors="coerce"
)
# для тех, где формат оказался другим — пробуем fallback
mask = df["timestamp"].isna()
if mask.any():
    df.loc[mask, "timestamp"] = pd.to_datetime(
        df.loc[mask, "timestamp"], errors="coerce"
    )

df = df.dropna(subset=["timestamp"])

# инициатор вида "anonymous/10.167.111.189" или "user1/10.0.0.2"
# извлекаем username и ip
df[["username", "ip"]] = (
    df["initiator"].astype(str).str.extract(r"^(?:(.+?)/)?(\d+\.\d+\.\d+\.\d+)$")
)
df["username"] = df["username"].fillna("anonymous")

# фильтр: исключаем записи, где username содержит "*TASK"
df = df[~df["username"].str.contains(r"\*TASK", na=False)]

# имя репозитория
df["repo"] = df["attributes.repository.name"]

# === 3. Определение обращения (сессии) ===
df = df.sort_values(by=["ip", "repo", "timestamp"])
max_interval = timedelta(minutes=2)

df["prev_time"] = df.groupby(["ip", "repo"])["timestamp"].shift(1)
df["time_diff"] = df["timestamp"] - df["prev_time"]
df["new_session"] = (df["time_diff"] > max_interval) | df["time_diff"].isna()
df["session_id"] = df.groupby(["ip", "repo"])["new_session"].cumsum()

# === 4. Определяем тип обращения ===
df["is_download"] = df["type"].str.contains("downloaded", case=False, na=False)
df["is_upload"] = df["type"].str.contains("created|uploaded", case=False, na=False)

# анонимные считаем скачиваниями (по твоему правилу)
df.loc[df["username"] == "anonymous", "is_download"] = True

# === 5. Формируем обращения (агрегация по session) ===
sessions = (
    df.groupby(["ip", "repo", "session_id"])
    .agg(
        {
            "timestamp": ["min", "max"],
            "is_download": "max",
            "is_upload": "max",
            "username": lambda x: x.dropna().iloc[0]
            if any(x.dropna())
            else "anonymous",
        }
    )
    .reset_index()
)
sessions.columns = [
    "ip",
    "repo",
    "session_id",
    "start_time",
    "end_time",
    "is_download",
    "is_upload",
    "username",
]

# === 6. Собираем IP по username ===
user_ips = (
    sessions.groupby("username")["ip"]
    .unique()
    .apply(lambda arr: sorted(list(arr)))
    .reset_index()
    .rename(columns={"ip": "ip_list"})
)

# === 7. Сводная статистика по репозиториям ===
repo_stats = (
    sessions.groupby("repo")
    .agg(
        pulls=("is_download", "sum"),
        pushes=("is_upload", "sum"),
        total_sessions=("session_id", "count"),
        total_users=("username", pd.Series.nunique),
    )
    .reset_index()
)

# === 8. Разделение пользователей на обычных и анонимных ===
anon_names = {"anonymous", "*UNKNOWN"}
users_normal = user_ips[~user_ips["username"].isin(anon_names)].copy()
users_anonymous = user_ips[user_ips["username"].isin(anon_names)].copy()

# === 9. Вывод в требуемом формате ===
print("=== Сводка по репозиториям ===")
print(repo_stats.to_string(index=False))

print("\n=== Уникальные пользователи ===")
print(f"Обычные пользователей: {len(users_normal)}")
print(
    f"Анонимных (IP отдельных записей): {sum(len(ips) for ips in users_anonymous['ip_list'])}"
)

print("\n=== Список обычных пользователей и их IP ===")
# Формат: имя (ip1, ip2, ...)
for _, row in users_normal.sort_values("username").iterrows():
    name = row["username"]
    ips = ", ".join(row["ip_list"])
    print(f"{name} ({ips})")

print("\n=== Список анонимусов (по одному IP на строку) ===")
# Для анонимных распечатываем по одному IP на строку: anonymous (ip)
for _, row in users_anonymous.iterrows():
    for ip in row["ip_list"]:
        print(f"{row['username']} ({ip})")

# === Опционально: сохранить CSV-файлы рядом со скриптом ===
repo_stats.to_csv("repo_stats.csv", index=False)
user_ips.to_csv("user_ips.csv", index=False)
sessions.to_csv("sessions.csv", index=False)
print("\nCSV сохранены: repo_stats.csv, user_ips.csv, sessions.csv")
