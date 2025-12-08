import sqlite3
import pandas as pd
from datetime import timedelta, datetime
from dateutil import parser
import logging

from config import MAX_SESSION_INTERVAL_MIN, MERGE_SESSION_GAP_MIN


log = logging.getLogger("log_filter")


"""
Анализ логов Nexus: группировка обращений пользователей по репозиториям.

Механизм работы:
----------------
1. Логи сортируются по инициатору, репозиторию и времени.
2. Каждое обращение (session) — это последовательность запросов
   от одного инициатора к одному репозиторию без пауз дольше max_interval.
3. Если пауза между соседними обращениями меньше merge_gap —
   они считаются одной логической сессией.

Параметры:
-----------
max_interval : timedelta
    Максимально допустимая пауза внутри одной сессии.
merge_gap : timedelta
    Допустимая пауза между соседними сессиями для объединения.

Итог:
------
Возвращается структура словарей с таблицами:
- repo_stats        (сводка по репозиториям)
- users_by_repo     (список пользователей по репозиторию)
- normal_users      (таблица пользователь → IP)
- anonymous_users   (анонимные запросы)
- sessions          (конечные объединённые сессии)
"""


# ============================================================
# Умный парсер времён
# ============================================================

KNOWN_FORMATS = [
    "%Y-%m-%d %H:%M:%S,%f%z",  # пример: 2025-08-26 00:00:00,035+0000
    "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO-8601
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
]


def parse_ts_smart(ts):
    ts = str(ts).strip()
    if not ts:
        return None

    for fmt in KNOWN_FORMATS:
        try:
            return datetime.strptime(ts, fmt)
        except:
            pass

    try:
        return parser.parse(ts)
    except:
        return None


# ============================================================
# Извлечение username / IP из initiator
# ============================================================


def extract_user_ip(initiator: str):
    """
    Поддерживает:
    - user/ip
    - только ip
    - только user
    - unknown / empty
    """
    s = str(initiator).strip()

    # user/ip
    if "/" in s:
        user, ip = s.split("/", 1)
        return user or "anonymous", ip

    # чистый IP
    if s.count(".") == 3 and all(x.isdigit() for x in s.split(".")):
        return "anonymous", s

    # user без IP
    if s:
        return s, None

    return "anonymous", None


# ============================================================
# Основная функция обработки логов
# ============================================================


def process_logs(db_path: str):
    log.info("Чтение raw_logs из SQLite")

    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT timestamp, initiator, repo FROM raw_logs", conn)
    conn.close()

    log.info(f"Всего записей: {len(df)}")

    # ----------------------------------------------------
    # Фильтрация task-записей
    # ----------------------------------------------------
    df = df[~df["initiator"].astype(str).str.contains("task", case=False, na=False)]
    df = df[df["repo"].notna()]

    log.info(f"После фильтрации системных записей: {len(df)}")

    # ----------------------------------------------------
    # Парсинг timestamp
    # ----------------------------------------------------
    df["timestamp"] = df["timestamp"].apply(parse_ts_smart)
    df = df.dropna(subset=["timestamp"])

    log.info(f"После обработки timestamp: {len(df)}")

    # ----------------------------------------------------
    # Нормализация initiator → username / ip
    # ----------------------------------------------------
    df[["username", "ip"]] = df["initiator"].apply(
        lambda x: pd.Series(extract_user_ip(x))
    )

    # ----------------------------------------------------
    # Сортировка
    # ----------------------------------------------------
    df = df.sort_values(by=["username", "repo", "timestamp"])

    # ============================================================
    # Формирование первичных сессий
    # ============================================================

    max_interval = timedelta(minutes=MAX_SESSION_INTERVAL_MIN)

    df["new_session"] = (
        (df["username"] != df["username"].shift())
        | (df["repo"] != df["repo"].shift())
        | ((df["timestamp"] - df["timestamp"].shift()) > max_interval)
    )

    df["session_id"] = df["new_session"].cumsum()

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

    merge_gap = timedelta(minutes=MERGE_SESSION_GAP_MIN)

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
    # Статистика
    # ============================================================

    repo_stats = (
        merged.groupby("repo")
        .agg(
            total_sessions=("merged_id", "count"),
            unique_users=("username", pd.Series.nunique),
        )
        .reset_index()
    )

    users_by_repo = (
        merged.groupby("repo")
        .apply(lambda x: sorted(set(x["username"])))
        .reset_index(name="users")
    )

    normal_users = merged[merged["username"] != "anonymous"]
    anonymous_users = merged[merged["username"] == "anonymous"]

    return {
        "sessions": merged,
        "repo_stats": repo_stats,
        "users_by_repo": users_by_repo,
        "normal_users": normal_users,
        "anonymous_users": anonymous_users,
    }
