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


def build_full_report(
    log_stats,
    ad_repo_map,
    repo_sizes,
    users_with_groups,
    bk_users,  # tuple: (matched, no_email, not_found)
    output_file,
    db_path,
):
    log.info(f"Создаём Excel: {output_file}")

    matched, no_email, not_found = bk_users

    # =============================
    # 1. AD Repo Usage (repo → size + ad_groups)
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
    # 3. BK Users (три секции)
    # =============================
    df_bk_matched = pd.DataFrame(matched)
    df_bk_no_email = pd.DataFrame(no_email)
    df_bk_not_found = pd.DataFrame(not_found)

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
        # AD Repo Usage
        df_ad_usage.to_excel(writer, sheet_name="AD Repo Usage", index=False)

        # AD Users
        df_ad_users.to_excel(writer, sheet_name="AD Users", index=False)

        # BK Users — вручную, секциями
        workbook = writer.book
        ws_bk = workbook.add_worksheet("BK Users")
        writer.sheets["BK Users"] = ws_bk

        start_row = 0

        # FOUND
        if not df_bk_matched.empty:
            ws_bk.write(start_row, 0, "FOUND IN BK")
            df_bk_matched.to_excel(
                writer,
                sheet_name="BK Users",
                startrow=start_row + 1,
                index=False,
            )
            start_row += len(df_bk_matched) + 3
        else:
            ws_bk.write(start_row, 0, "FOUND IN BK: NO DATA")
            start_row += 2

        # TECH ACCOUNTS (NO EMAIL)
        if not df_bk_no_email.empty:
            ws_bk.write(start_row, 0, "TECH ACCOUNTS (NO EMAIL)")
            df_bk_no_email.to_excel(
                writer,
                sheet_name="BK Users",
                startrow=start_row + 1,
                index=False,
            )
            start_row += len(df_bk_no_email) + 3
        else:
            ws_bk.write(start_row, 0, "TECH ACCOUNTS (NO EMAIL): NO DATA")
            start_row += 2

        # NOT FOUND (FIRED)
        if not df_bk_not_found.empty:
            ws_bk.write(start_row, 0, "NOT FOUND IN BK (FIRED)")
            df_bk_not_found.to_excel(
                writer,
                sheet_name="BK Users",
                startrow=start_row + 1,
                index=False,
            )
            start_row += len(df_bk_not_found) + 3
        else:
            ws_bk.write(start_row, 0, "NOT FOUND IN BK (FIRED): NO DATA")
            start_row += 2

        # Сводка по репозиториям
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

        # Пользователи по репозиторию
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

        # Обычные пользователи
        df_normal = prepend_instruction(
            users_normal,
            [
                "Список зарегистрированных пользователей и IP-адресов, с которых они подключались.",
                "",
            ],
        )
        df_normal.to_excel(writer, sheet_name="Обычные пользователи", index=False)

        # Анонимные пользователи
        df_anon = prepend_instruction(
            users_anonymous_flat,
            ["Анонимные подключения без логина. Каждый IP — отдельная строка.", ""],
        )
        df_anon.to_excel(writer, sheet_name="Анонимные пользователи", index=False)

        # -------------------------
        # Автоширина колонок
        # -------------------------
        # Для BK Users — берём первую не пустую таблицу, чтобы оценить колонки
        bk_cols_source = None
        for df_src in (df_bk_matched, df_bk_no_email, df_bk_not_found):
            if not df_src.empty:
                bk_cols_source = df_src
                break

        for sheet_name, df_tmp in {
            "AD Repo Usage": df_ad_usage,
            "AD Users": df_ad_users,
            "Сводка по репозиториям": df_repo_summary,
            "Пользователи по репозиторию": df_repo_users,
            "Обычные пользователи": df_normal,
            "Анонимные пользователи": df_anon,
        }.items():
            worksheet = writer.sheets[sheet_name]
            for i, col in enumerate(df_tmp.columns):
                max_len = max(len(str(col)), df_tmp[col].astype(str).map(len).max()) + 2
                worksheet.set_column(i, i, max_len)

        # Автоширина для BK Users
        if bk_cols_source is not None:
            for i, col in enumerate(bk_cols_source.columns):
                max_len = len(str(col))
                for df_src in (df_bk_matched, df_bk_no_email, df_bk_not_found):
                    if col in df_src.columns and not df_src.empty:
                        col_max = df_src[col].astype(str).map(len).max()
                        if col_max > max_len:
                            max_len = col_max
                ws_bk.set_column(i, i, max_len + 2)

    log.info("Excel готов")

    # =============================
    # Удаление временных директорий
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
