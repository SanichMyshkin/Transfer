import os
import shutil
import pandas as pd
from pathlib import Path
from config import REPORT_PATH


# =====================================================
#  Удаление временных директорий
# =====================================================

def cleanup_temp_dirs():
    paths = [Path("temp_extract"), Path("temp_db")]
    for p in paths:
        if p.exists():
            try:
                shutil.rmtree(p, ignore_errors=True)
                print(f"[CLEANUP] Удалена временная директория: {p}")
            except Exception as e:
                print(f"[CLEANUP] Не удалось удалить {p}: {e}")


# =====================================================
#  Убираем timezone из datetime (Excel не поддерживает TZ)
# =====================================================

def strip_tz(df: pd.DataFrame):
    """Удаляет TZ из всех datetime колонок DataFrame."""
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = df[col].dt.tz_localize(None)
            except Exception:
                pass
    return df


# =====================================================
#  Универсальная запись DataFrame в Excel
# =====================================================

def write_sheet(writer, sheet_name: str, df: pd.DataFrame):
    """Безопасная запись листа с автошириной."""
    df = strip_tz(df.copy())

    df.to_excel(writer, sheet_name=sheet_name, index=False)
    worksheet = writer.sheets[sheet_name]

    # автоширина
    for idx, col in enumerate(df.columns):
        column_len = max(
            len(str(col)),
            df[col].astype(str).apply(len).max() if len(df) else len(str(col))
        )
        worksheet.set_column(idx, idx, column_len + 2)


# =====================================================
#  Главная функция формирования Excel отчёта
# =====================================================

def build_excel_report(
    repo_sizes,
    repo_data,
    role_repo_map,
    ad_map,
    log_stats,
    output_file=REPORT_PATH
):
    """
    Формирует многостраничный Excel-отчёт:
    - Repo Sizes
    - Repository Info
    - Roles → AD Groups
    - Role → Repositories
    - Repository Stats
    - Users by Repository
    - Normal Users
    - Anonymous Users
    - Sessions (полная разбивка)
    """

    print("[REPORT] Формируем Excel отчёт...")

    # ---------------------------------------------
    # Подготовка данных
    # ---------------------------------------------

    df_repo_sizes = pd.DataFrame(
        [{"repository": repo, "size_bytes": size} for repo, size in repo_sizes.items()]
    )

    df_repo_info = pd.DataFrame(repo_data)

    df_roles = pd.DataFrame([
        {
            "role_id": role,
            "ad_group": ad_map.get(role, ""),
            "repositories": ", ".join(repos)
        }
        for role, repos in role_repo_map.items()
    ])

    df_repo_stats = log_stats["repo_stats"]
    df_users_by_repo = log_stats["users_by_repo"]
    df_normal_users = log_stats["normal_users"]
    df_anon_users = log_stats["anonymous_users"]
    df_sessions = log_stats["sessions"]

    # ---------------------------------------------
    # Генерация Excel
    # ---------------------------------------------
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:

        write_sheet(writer, "Repo Sizes", df_repo_sizes)
        write_sheet(writer, "Repo Info", df_repo_info)
        write_sheet(writer, "Roles", df_roles)

        write_sheet(writer, "Repo Stats", df_repo_stats)
        write_sheet(writer, "Users by Repo", df_users_by_repo)

        write_sheet(writer, "Normal Users", df_normal_users)
        write_sheet(writer, "Anonymous Users", df_anon_users)
        write_sheet(writer, "Sessions", df_sessions)

    print(f"[REPORT] Excel сохранён: {output_file}")

    # ---------------------------------------------
    # Удаляем временные директории
    # ---------------------------------------------
    cleanup_temp_dirs()

    print("[REPORT] Отчёт завершён, временные файлы очищены.")
