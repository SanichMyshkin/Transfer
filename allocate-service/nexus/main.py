import os
import shutil
import logging
import sqlite3
from datetime import timedelta
from pathlib import Path

import pandas as pd

from log_loader import load_all_audit_logs

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


# ============================================
# ЛОГИРОВАНИЕ
# ============================================

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("main")


"""
Анализ логов Nexus: группировка обращений пользователей по репозиториям.
"""


def parse_log_record(d: dict):
    """
    Приведение сырой записи (dict) к целевому формату или пропуск.

    Здесь d — это запись из SQLite (колонки: id, timestamp, initiator, repo).
    """
    initiator = d.get("initiator", "")

    # Пропускаем системные задания Nexus (*TASK, system, scheduled и т.п.)
    if isinstance(initiator, str) and "task" in initiator.lower():
        return None
    # domain/type в raw_logs мы не сохраняли, так что эти фильтры уже не сработают,
    # но оставим на будущее, если будешь расширять сохранение полей.
    if d.get("domain") == "tasks" or d.get("type") == "scheduled":
        return None

    repo_name = d.get("repo")

    return {
        "timestamp": d.get("timestamp"),
        "initiator": initiator,
        "repo": repo_name,
    }


def prepend_instruction(df, text_lines):
    """Добавляет строки-инструкции перед таблицей, без смещения столбцов."""
    blank_row = {col: None for col in df.columns}
    instruction_rows = []
    for line in text_lines:
        row = blank_row.copy()
        first_col = list(df.columns)[0]
        row[first_col] = line
        instruction_rows.append(row)
    return pd.concat([pd.DataFrame(instruction_rows), df], ignore_index=True)


def main():
    # ============================================
    # 1. Загрузка логов → SQLite
    # ============================================

    ARCHIVE_PATH = "path/to/big_archive.zip"  # TODO: поменяй на реальный путь

    log.info("Старт анализа логов")
    log.info(f"Архив: {ARCHIVE_PATH}")

    db_path = load_all_audit_logs(ARCHIVE_PATH)
    log.info(f"Временная SQLite-база создана: {db_path}")

    # ============================================
    # 2. Чтение сырых логов из SQLite
    # ============================================

    log.info("Читаем данные из SQLite")
    conn = sqlite3.connect(db_path)

    # raw_logs: id, timestamp, initiator, repo
    df_raw = pd.read_sql_query(
        "SELECT id, timestamp, initiator, repo FROM raw_logs", conn
    )
    conn.close()

    log.info(f"Всего строк в raw_logs: {len(df_raw)}")

    # ============================================
    # 3. Фильтрация и приведение к рабочему формату
    # ============================================

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

    # ============================================
    # 4. Приведение временных меток
    # ============================================

    log.info("Преобразуем timestamp в datetime")

    df["timestamp"] = pd.to_datetime(
        df["timestamp"], format="%Y-%m-%d %H:%M:%S,%f%z", errors="coerce"
    )

    before = len(df)
    df = df.dropna(subset=["timestamp"])
    after = len(df)

    log.info(f"Удалено строк с некорректным timestamp: {before - after}")

    # Разбивка initiator на username / ip
    log.info("Извлекаем username и IP из initiator")

    df[["username", "ip"]] = (
        df["initiator"].astype(str).str.extract(r"^(?:(.+?)/)?(\d+\.\d+\.\d+\.\d+)$")
    )

    df["username"] = df["username"].fillna("anonymous")
    df = df[~df["username"].str.contains(r"\*TASK", na=False)]

    df = df.sort_values(by=["initiator", "repo", "timestamp"])
    log.info(f"После сортировки и фильтрации осталось {len(df)} строк")

    # ============================================
    # 5. Формирование первичных сессий
    # ============================================

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

    # ============================================
    # 6. Объединение коротких подряд идущих сессий
    # ============================================

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

    # ============================================
    # 7. Определение идентификатора пользователя
    # ============================================

    log.info("Формируем user_identity")

    sessions["user_identity"] = sessions.apply(
        lambda r: (
            r["username"] if r["username"] not in {"anonymous", "*UNKNOWN"} else r["ip"]
        ),
        axis=1,
    )

    # ============================================
    # 8. Сводка по репозиториям
    # ============================================

    log.info("Считаем сводку по репозиториям")

    repo_stats = (
        sessions.groupby("repo")
        .agg(
            total_requests=("merged_session_id", "count"),
            total_users=("user_identity", pd.Series.nunique),
        )
        .reset_index()
    )

    # ============================================
    # 9. Пользователи по каждому репозиторию
    # ============================================

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

    # ============================================
    # 10. Пользователи и IP
    # ============================================

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

    # ============================================
    # 11. Убираем временную зону (для Excel)
    # ============================================

    log.info("Удаляем временную зону из дат")

    for col in ["start_time", "end_time"]:
        if hasattr(sessions[col].dtype, "tz"):
            sessions[col] = sessions[col].dt.tz_localize(None)

    # ============================================
    # 12. Экспорт в Excel
    # ============================================

    output_file = "nexus_report.xlsx"
    log.info(f"Создаём Excel: {output_file}")

    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        # --- 1. Сводка по репозиториям ---
        text_repo = [
            "Эта таблица показывает, сколько обращений было к каждому репозиторию.",
            "Поля: total_requests — количество обращений, total_users — уникальных пользователей.",
            "",
        ]
        df_repo = prepend_instruction(repo_stats, text_repo)
        df_repo.to_excel(writer, sheet_name="Сводка по репозиториям", index=False)

        # --- 2. Пользователи по каждому репозиторию ---
        text_repo_users = [
            "Здесь видно, кто именно обращался к каждому репозиторию.",
            "Формат: username (ip). Если только IP — пользователь анонимный.",
            "",
        ]
        df_repo_users = prepend_instruction(repo_users, text_repo_users)
        df_repo_users.to_excel(
            writer, sheet_name="Пользователи по репозиторию", index=False
        )

        # --- 3. Обычные пользователи ---
        text_normal = [
            "Список зарегистрированных пользователей и IP-адресов, с которых они подключались.",
            "",
        ]
        df_normal = prepend_instruction(users_normal, text_normal)
        df_normal.to_excel(writer, sheet_name="Обычные пользователи", index=False)

        # --- 4. Анонимные пользователи ---
        text_anon = [
            "Анонимные подключения без логина. Каждый IP — отдельная строка.",
            "",
        ]
        df_anon = prepend_instruction(users_anonymous_flat, text_anon)
        df_anon.to_excel(writer, sheet_name="Анонимные пользователи", index=False)

        # --- Автоширина ---
        all_sheets = {
            "Сводка по репозиториям": df_repo,
            "Пользователи по репозиторию": df_repo_users,
            "Обычные пользователи": df_normal,
            "Анонимные пользователи": df_anon,
        }

        for sheet_name, df_tmp in all_sheets.items():
            worksheet = writer.sheets[sheet_name]
            for i, col in enumerate(df_tmp.columns):
                max_len = max(len(str(col)), df_tmp[col].astype(str).map(len).max()) + 2
                worksheet.set_column(i, i, max_len)

    log.info("Excel готов")

    # ============================================
    # 13. Очистка временных файлов
    # ============================================

    log.info("Чистим временные файлы")

    try:
        os.remove(db_path)
        log.info(f"Удалена временная база: {db_path}")
    except Exception as e:
        log.warning(f"Не удалось удалить {db_path}: {e}")

    for temp_dir in (Path("temp_extract"), Path("temp_db")):
        if temp_dir.exists():
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
                log.info(f"Удалена временная директория: {temp_dir}")
            except Exception as e:
                log.warning(f"Не удалось удалить {temp_dir}: {e}")

    log.info("Завершено")


if __name__ == "__main__":
    main()
