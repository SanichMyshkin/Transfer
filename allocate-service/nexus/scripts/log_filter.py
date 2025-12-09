import sqlite3
import logging
from datetime import timedelta

import pandas as pd

log = logging.getLogger("log_filter")


def parse_log_record(d: dict):
    initiator = d.get("initiator", "")

    if isinstance(initiator, str) and "task" in initiator.lower():
        return None
    if d.get("domain") == "tasks" or d.get("type") == "scheduled":
        return None

    repo_name = d.get("repo")

    return {
        "timestamp": d.get("timestamp"),
        "initiator": initiator,
        "repo": repo_name,
    }


def analyze_logs(db_path: str):
    log.info("Читаем данные из SQLite")
    conn = sqlite3.connect(db_path)

    df_raw = pd.read_sql_query(
        "SELECT id, timestamp, initiator, repo FROM raw_logs", conn
    )
    conn.close()

    log.info(f"Всего строк в raw_logs: {len(df_raw)}")

    log.info("Фильтруем и приводим записи к рабочему виду")

    records = []
    for raw in df_raw.to_dict(orient="records"):
        parsed = parse_log_record(raw)
        if parsed:
            records.append(parsed)

    df = pd.DataFrame(records)
    log.info(f"После фильтрации валидных записей: {len(df)}")

    if df.empty:
        log.error("Не найдено ни одной валидной записи")
        raise SystemExit("Нет данных для анализа")

    log.info("Преобразуем timestamp в datetime")

    df["timestamp"] = pd.to_datetime(
        df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f%z", errors="coerce"
    )

    before = len(df)
    df = df.dropna(subset=["timestamp"])
    after = len(df)

    log.info(f"Удалено строк с некорректным timestamp: {before - after}")
    log.info("Извлекаем username и IP из initiator")

    df[["username", "ip"]] = df["initiator"].str.extract(
        r"^([^/]+)/(\d+\.\d+\.\d+\.\d+)$"
    )

    df["username"] = df["username"].fillna("anonymous")
    df = df[~df["username"].str.contains(r"\*TASK", na=False)]

    df = df.sort_values(by=["initiator", "repo", "timestamp"])
    log.info(f"После сортировки и фильтрации осталось {len(df)} строк")
    log.info("Формируем первичные сессии")

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

    log.info(f"Первичных сессий: {len(sessions)}")
    log.info("Объединяем короткие подряд идущие сессии")

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

    log.info(f"Сессий после объединения: {len(sessions)}")
    log.info("Формируем user_identity")

    sessions["user_identity"] = sessions.apply(
        lambda r: (
            r["username"] if r["username"] not in {"anonymous", "*UNKNOWN"} else r["ip"]
        ),
        axis=1,
    )

    log.info("Считаем сводку по репозиториям")

    repo_stats = (
        sessions.groupby("repo")
        .agg(
            total_requests=("merged_session_id", "count"),
            total_users=("user_identity", pd.Series.nunique),
        )
        .reset_index()
    )

    log.info("Формируем список пользователей по репозиторию")

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

    log.info("Формируем список пользователей и их IP")

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

    return {
        "sessions": sessions,
        "repo_stats": repo_stats,
        "repo_users": repo_users,
        "users_normal": users_normal,
        "users_anonymous_flat": users_anonymous_flat,
    }
