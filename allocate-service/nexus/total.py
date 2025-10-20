import pandas as pd
import json
from datetime import timedelta


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
    with open(filename, "r", encoding="utf-8") as f:
        records = [parse_log_line(line.strip()) for line in f if line.strip()]
    df = pd.DataFrame([r for r in records if r])
    if df.empty:
        raise SystemExit(f"Файл {filename} пуст или не содержит корректных JSON-записей.")
    return df


df = load_logs("trash/nexus.jsonl")

df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
df = df.dropna(subset=["timestamp"])

df[["username", "ip"]] = df["initiator"].astype(str).str.extract(r"^(?:(.+?)/)?(\d+\.\d+\.\d+\.\d+)$")
df["username"] = df["username"].fillna("anonymous")
df = df[~df["username"].str.contains(r"\*TASK", na=False)]

df = df.sort_values(by=["initiator", "repo", "timestamp"])
max_interval = timedelta(seconds=300)
df["gap"] = (
    (df["initiator"] != df["initiator"].shift()) |
    (df["repo"] != df["repo"].shift()) |
    ((df["timestamp"] - df["timestamp"].shift()) > max_interval)
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

user_ips = (
    sessions.groupby("username")["ip"]
    .apply(lambda x: sorted([ip for ip in set(x) if pd.notna(ip)]))
    .reset_index()
    .rename(columns={"ip": "ip_list"})
)

repo_stats = (
    sessions.groupby("repo")
    .agg(
        total_requests=("session_id", "count"),
        total_users=("username", pd.Series.nunique),
    )
    .reset_index()
)

anon_names = {"anonymous", "*UNKNOWN"}
users_normal = user_ips[~user_ips["username"].isin(anon_names)]
users_anonymous = user_ips[user_ips["username"].isin(anon_names)]

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
        print(f"{row['username']} ({', '.join(row['ip_list'])})")

print("\n=== Список анонимусов (по одному IP на строку) ===")
if users_anonymous.empty:
    print("Нет анонимных обращений.")
else:
    for _, row in users_anonymous.iterrows():
        for ip in row["ip_list"]:
            print(f"{row['username']} ({ip})")
