import sqlite3
import logging
from datetime import timedelta
import pandas as pd


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

log = logging.getLogger("log_filter")


def parse_log_record(d: dict):
    initiator = d.get("initiator", "")

    if isinstance(initiator, str) and "task" in initiator.lower():
        return None
    if d.get("domain") == "tasks" or d.get("type") == "scheduled":
        return None

    return {
        "timestamp": d.get("timestamp"),
        "initiator": initiator,
        "repo": d.get("repo"),
    }


def analyze_logs(db_path: str):
    log.info("Читаем данные из SQLite")
    conn = sqlite3.connect(db_path)
    df_raw = pd.read_sql_query(
        "SELECT id, timestamp, initiator, repo FROM raw_logs", conn
    )
    conn.close()

    log.info(f"Всего строк в raw_logs: {len(df_raw)}")

    # Фильтрация
    records = []
    for raw in df_raw.to_dict(orient="records"):
        parsed = parse_log_record(raw)
        if parsed:
            records.append(parsed)

    df = pd.DataFrame(records)
    log.info(f"После фильтрации валидных записей: {len(df)}")

    if df.empty:
        raise SystemExit("Нет данных для анализа")

    # timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    # → username/ip (ТВОЁ ПРАВИЛО) ←
    df[["username", "ip"]] = (
        df["initiator"].astype(str).str.extract(r"^(?:(.+?)/)?(\d+\.\d+\.\d+\.\d+)$")
    )

    # Если формат НЕ username/ip → НЕ пользователь → anonymous
    df["username"] = df["username"].fillna("anonymous")

    df = df.sort_values(by=["initiator", "repo", "timestamp"])

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

    merge_gap = timedelta(minutes=1)
    sessions = sessions.sort_values(by=["initiator", "repo", "start_time"])
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

    # user_identity
    sessions["user_identity"] = sessions.apply(
        lambda r: r["username"]
        if r["username"] not in {"anonymous", "*UNKNOWN"}
        else r["ip"],
        axis=1,
    )

    repo_stats = (
        sessions.groupby("repo")
        .agg(
            total_requests=("merged_session_id", "count"),
            total_users=("user_identity", pd.Series.nunique),
        )
        .reset_index()
    )

    return {
        "sessions": sessions,
        "repo_stats": repo_stats,
    }
