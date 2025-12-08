# log_filter.py
import sqlite3
import pandas as pd
import logging
from datetime import timedelta

log = logging.getLogger("log_filter")

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
На выходе формируется структура данных для отчёта с таблицами:
- Сводка по репозиториям
- Пользователи по каждому репозиторию
- Обычные пользователи (логины + IP)
- Анонимные пользователи (IP)
"""


# ============================================================
# Нормализация initiator
# ============================================================


def extract_user_ip(initiator: str):
    """
    Возвращает (username, ip).
    Поддерживает: user/ip, IP-only, UNKNOWN, anonymous
    """
    s = str(initiator)

    if "/" in s:
        user, ip = s.split("/", 1)
        return user or "anonymous", ip

    # только IP
    if s.count(".") == 3:
        return "anonymous", s

    return s or "anonymous", None


# ============================================================
# Основная функция
# ============================================================


def process_logs(db_path: str):
    log.info("Чтение SQLite → DataFrame")
    conn = sqlite3.connect(db_path)

    df_raw = pd.read_sql_query(
        "SELECT id, timestamp, initiator, repo FROM raw_logs", conn
    )
    conn.close()

    log.info(f"Всего строк raw_logs: {len(df_raw)}")

    # фильтруем task-записи
    df = df_raw[
        ~df_raw["initiator"].astype(str).str.contains("task", case=False, na=False)
    ]

    log.info(f"После фильтрации: {len(df)} строк")

    # timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    # user/ip
    df[["username", "ip"]] = df["initiator"].apply(
        lambda x: pd.Series(extract_user_ip(x))
    )

    df = df.sort_values(by=["username", "repo", "timestamp"])

    # ============================================================
    # Формирование сессий
    # ============================================================

    log.info("Выделение первичных сессий")

    max_interval = timedelta(minutes=5)

    df["new_session"] = (
        (df["username"] != df["username"].shift())
        | (df["repo"] != df["repo"].shift())
        | ((df["timestamp"] - df["timestamp"].shift()) > max_interval)
    )

    df["session_id"] = df["new_session"].cumsum()

    # сводка по сессиям
    sessions = (
        df.groupby(["username", "repo", "session_id"])
        .agg(
            start_time=("timestamp", "min"),
            end_time=("timestamp", "max"),
            ip=("ip", "first"),
        )
        .reset_index()
    )

    # ============================================================
    # Объединение коротких сессий
    # ============================================================

    merge_gap = timedelta(minutes=1)

    sessions = sessions.sort_values(["username", "repo", "start_time"])

    sessions["is_new"] = (
        (sessions["username"] != sessions["username"].shift())
        | (sessions["repo"] != sessions["repo"].shift())
        | ((sessions["start_time"] - sessions["end_time"].shift()) > merge_gap)
    )

    sessions["merged_id"] = sessions["is_new"].cumsum()

    merged = (
        sessions.groupby(["username", "repo", "merged_id"])
        .agg(
            start_time=("start_time", "min"),
            end_time=("end_time", "max"),
            ip=("ip", "first"),
        )
        .reset_index()
    )

    # ============================================================
    # Статистика по репозиториям
    # ============================================================

    repo_stats = (
        merged.groupby("repo")
        .agg(
            total_sessions=("merged_id", "count"),
            unique_users=("username", pd.Series.nunique),
        )
        .reset_index()
    )

    # ============================================================
    # Пользователи по репозиторию
    # ============================================================

    users_by_repo = (
        merged.groupby("repo")
        .apply(lambda x: sorted(set(x["username"])))
        .reset_index(name="users")
    )

    # ============================================================
    # Обычные и анонимные пользователи
    # ============================================================

    normal_users = merged[merged["username"] != "anonymous"]
    anonymous_users = merged[merged["username"] == "anonymous"]

    return {
        "sessions": merged,
        "repo_stats": repo_stats,
        "users_by_repo": users_by_repo,
        "normal_users": normal_users,
        "anonymous_users": anonymous_users,
    }
