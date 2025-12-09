import os
import shutil
import logging
from pathlib import Path
import pandas as pd

log = logging.getLogger("excel_report")


def prepend_instruction(df, text_lines):
    if df is None or df.empty:
        return df

    blank_row = {col: None for col in df.columns}
    rows = []
    for line in text_lines:
        row = blank_row.copy()
        row[list(df.columns)[0]] = line
        rows.append(row)

    return pd.concat([pd.DataFrame(rows), df], ignore_index=True)


def assemble_bk_users_sheet(matched, no_email, not_found):
    """
    Формирует ОДИН лист BK Users со структурой:

    === FOUND IN BK ===
    данные...

    === TECH ACCOUNTS ===
    данные...

    === NOT FOUND (FIRED) ===
    данные...
    """

    rows = []

    def add_separator(text):
        rows.append({"__SECTION__": text})

    # FOUND
    add_separator("=== FOUND IN BK ===")
    for u in matched:
        row = {k: v for k, v in u.items() if k != "__CATEGORY__"}
        rows.append(row)

    # TECH ACCOUNTS
    add_separator("=== TECH ACCOUNTS ===")
    for u in no_email:
        row = {k: v for k, v in u.items() if k != "__CATEGORY__"}
        rows.append(row)

    # NOT FOUND
    add_separator("=== NOT FOUND (FIRED) ===")
    for u in not_found:
        row = {k: v for k, v in u.items() if k != "__CATEGORY__"}
        rows.append(row)

    df = pd.DataFrame(rows)

    # Первая колонка всегда __SECTION__ либо нормальные поля
    if "__SECTION__" not in df.columns:
        df["__SECTION__"] = None

    # Переносим __SECTION__ в начало листа
    cols = ["__SECTION__"] + [c for c in df.columns if c != "__SECTION__"]
    df = df[cols]

    return df


def build_full_report(
    log_stats,
    ad_repo_map,
    repo_sizes,
    users_with_groups,
    bk_users,  # теперь tuple: (matched, no_email, not_found)
    output_file,
    db_path,
):
    log.info(f"Создаём Excel: {output_file}")

    matched, no_email, not_found = bk_users

    # =============================
    # 1. AD Repo Usage
    # =============================
    ad_rows = []
    for repo, groups in ad_repo_map.items():
        size_info = repo_sizes.get(repo, {"size_human": "0 B"})
        ad_rows.append(
            {
                "repository": repo,
                "size": size_info["size_human"],
                "ad_groups": ", ".join(groups),
            }
        )

    df_ad_usage = pd.DataFrame(ad_rows)

    # =============================
    # 2. AD Users
    # =============================
    df_ad_users = pd.DataFrame(users_with_groups)

    # =============================
    # 3. BK Users
    # =============================
    df_bk_users = assemble_bk_users_sheet(matched, no_email, not_found)

    # =============================
    # 4. ЛОГИ
    # =============================
    sessions = log_stats["sessions"]
    repo_stats = log_stats["repo_stats"]
    repo_users = log_stats["repo_users"]
    users_normal = log_stats["users_normal"]
    users_anonymous_flat = log_stats["users_anonymous_flat"]

    for col in ["start_time", "end_time"]:
        if col in sessions.columns and hasattr(sessions[col].dtype, "tz"):
            sessions[col] = sessions[col].dt.tz_localize(None)

    # =============================
    # 5. Excel запись
    # =============================
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        df_ad_usage.to_excel(writer, sheet_name="AD Repo Usage", index=False)
        df_ad_users.to_excel(writer, sheet_name="AD Users", index=False)
        df_bk_users.to_excel(writer, sheet_name="BK Users", index=False)

        df_repo_summary = prepend_instruction(
            repo_stats,
            [
                "Эта таблица показывает, сколько обращений было к каждому репозиторию.",
                "Поля: total_requests — количество обращений, total_users — уникальных пользователей.",
                "",
            ],
        )
        df_repo_summary.to_excel(
            writer, sheet_name="Сводка по репозиториям", index=False
        )

        df_repo_users = prepend_instruction(
            repo_users,
            [
                "Здесь видно, кто именно обращался к каждому репозиторию.",
                "Формат: username (ip). Если только IP — пользователь анонимный.",
                "",
            ],
        )
        df_repo_users.to_excel(
            writer, sheet_name="Пользователи по репозиторию", index=False
        )

        df_normal = prepend_instruction(
            users_normal,
            [
                "Список зарегистрированных пользователей и IP-адресов, с которых они подключались.",
                "",
            ],
        )
        df_normal.to_excel(writer, sheet_name="Обычные пользователи", index=False)

        df_anon = prepend_instruction(
            users_anonymous_flat,
            ["Анонимные подключения без логина. Каждый IP — отдельная строка.", ""],
        )
        df_anon.to_excel(writer, sheet_name="Анонимные пользователи", index=False)

        # -------------------------
        # Автоширина
        # -------------------------
        for sheet_name, df_tmp in {
            "AD Repo Usage": df_ad_usage,
            "AD Users": df_ad_users,
            "BK Users": df_bk_users,
            "Сводка по репозиториям": df_repo_summary,
            "Пользователи по репозиторию": df_repo_users,
            "Обычные пользователи": df_normal,
            "Анонимные пользователи": df_anon,
        }.items():
            worksheet = writer.sheets[sheet_name]
            for i, col in enumerate(df_tmp.columns):
                max_len = max(len(str(col)), df_tmp[col].astype(str).map(len).max()) + 2
                worksheet.set_column(i, i, max_len)

    log.info("Excel готов")

    # =============================
    # Очистка временных файлов
    # =============================
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
