import os
import shutil
import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger("excel_report")


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


def build_full_report(
    stats: dict,
    repo_sizes: dict,
    ad_group_repo_map: list[dict],
    output_file: str,
    db_path: str | Path,
):
    """
    Формирует Excel:

    1) AD Repo Usage      (ad_group | repository | size)
    2) Сводка по репозиториям
    3) Пользователи по репозиторию
    4) Обычные пользователи
    5) Анонимные пользователи

    И очищает временные файлы (db + temp_*).
    """

    sessions = stats["sessions"]
    repo_stats = stats["repo_stats"]
    repo_users = stats["repo_users"]
    users_normal = stats["users_normal"]
    users_anonymous_flat = stats["users_anonymous_flat"]

    # 1. AD Repo Usage
    log.info("Готовим лист AD Repo Usage")

    ad_rows = []
    for m in ad_group_repo_map:
        ad = m["ad_group"]
        repo = m["repository"]
        size_info = repo_sizes.get(repo, {"size_human": "0 B"})
        ad_rows.append({
            "ad_group": ad,
            "repository": repo,
            "size": size_info["size_human"],
        })
    df_ad_usage = pd.DataFrame(ad_rows)

    log.info(f"Создаём Excel: {output_file}")

    # убираем TZ у дат в sessions (как было)
    log.info("Удаляем временную зону из дат")
    for col in ["start_time", "end_time"]:
        if col in sessions.columns and hasattr(sessions[col].dtype, "tz"):
            sessions[col] = sessions[col].dt.tz_localize(None)

    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        # --- 0. AD Repo Usage ---
        df_ad_usage.to_excel(writer, sheet_name="AD Repo Usage", index=False)

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
            "AD Repo Usage": df_ad_usage,
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

    # Очистка временных файлов — как раньше
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
