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
2. Каждое обращение (session) — последовательность запросов от одного
   initiator к одному repo без пауз дольше max_interval.
3. Если пауза между сессиями меньше merge_gap — они объединяются.

Параметры:
-----------
MAX_SESSION_INTERVAL_MIN – допустимая пауза внутри одной сессии.
MERGE_SESSION_GAP_MIN    – допустимая пауза между соседними сессиями.

Итог:
------
Возвращает словарь:
- sessions        — итоговые объединённые сессии
- repo_stats      — количество обращений по репозиториям
- users_by_repo   — пользователи каждого репозитория
- normal_users    — пользователи с именами
- anonymous_users — записи, где initiator = IP
"""


# ============================================================
# Умный парсер timestamp
# ============================================================

KNOWN_FORMATS = [
    "%Y-%m-%d %H:%M:%S,%f%z",      # 2025-08-26 00:00:00,035+0000
    "%Y-%m-%dT%H:%M:%S.%fZ",       # ISO-8601
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
]

def parse_ts_smart(ts):
    ts = str(ts).strip()
    if not ts:
        return None

    # пробуем точные форматы
    for fmt in KNOWN_FORMATS:
        try:
            return datetime.strptime(ts, fmt)
        except:
            pass

    # fallback
    try:
        return parser.parse(ts)
    except:
        return None


# ============================================================
# Извлечение username / IP
# ============================================================

def extract_user_ip(initiator: str):
    """
    Разбирает строки формата:
    - "user/ip"
    - "ip"
    - "user"
    """
    s = str(initiator).strip()

    if "/" in s:
        user, ip = s.split("/", 1)
        return user or "anonymous", ip

    # чистый IPv4
    if s.count(".") == 3 and all(part.isdigit() for part in s.split(".")):
        return "anonymous", s

    if s:
        return s, None

    return "anonymous", None


# ============================================================
# Основная функция обработки логов
# ============================================================

def process_logs(db_path: str):
    log.info("Чтение raw_logs из SQLite")

    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        "SELECT timestamp, initiator, repo FROM raw_logs",
        conn
    )
    conn.close()

    log.info(f"Всего записей: {len(df)}")

    # ----------------------------------------------------
    # Фильтрация задач Nexus (task)
    # ----------------------------------------------------
    df = df[~df["initiator"].astype(str).str.contains("task", case=False, na=False)]
    df = df[df["repo"].notna()]

    log.info(f"После фильтрации системных задач: {len(df)}")

    # ----------------------------------------------------
    # Парсинг timestamp
    # ----------------------------------------------------
    df["timestamp"] = df["timestamp"].apply(parse_ts_smart)
    df = df.dropna(subset=["timestamp"])

    log.info(f"После нормализации timestamp: {len(df)}")

    # ----------------------------------------------------
    # Разбор initiator: username + ip
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
        (df["username"] != df["username"].shift()) |
        (df["repo"] != df["repo"].shift()) |
        ((df["timestamp"] - df["timestamp"].shift()) > max_interval)
    )

    df["session_id"] = df["new_session"].cumsum()

    sessions = (
        df.groupby(["username", "repo", "session_id"])
        .agg(
            start_time=("timestamp", "min"),
            end_time=("timestamp", "max"),
            ip=("ip", "first")
        )
        .reset_index()
    )

    # ============================================================
    # Объединение коротких последовательных сессий
    # ============================================================

    merge_gap = timedelta(minutes=MERGE_SESSION_GAP_MIN)

    sessions = sessions.sort_values(["username", "repo", "start_time"])

    sessions["is_new"] = (
        (sessions["username"] != sessions["username"].shift()) |
        (sessions["repo"] != sessions["repo"].shift()) |
        ((sessions["start_time"] - sessions["end_time"].shift()) > merge_gap)
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
            unique_users=("username", pd.Series.nunique)
        )
        .reset_index()
    )

    # ============================================================
    # Список пользователей по каждому репозиторию
    # (исправленный, без FutureWarning)
    # ============================================================

    users_by_repo = (
        merged.groupby("repo")["username"]
        .agg(lambda x: sorted(set(x)))
        .reset_index(name="users")
    )

    # ============================================================
    # Разделение пользователей
    # ============================================================

    normal_users = merged[merged["username"] != "anonymous"]
    anonymous_users = merged[merged["username"] == "anonymous"]

    return {
        "sessions": merged,
        "repo_stats": repo_stats,
        "users_by_repo": users_by_repo,
        "normal_users": normal_users,
        "anonymous_users": anonymous_users
    }
