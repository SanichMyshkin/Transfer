import shutil
import logging
import pandas as pd
from pathlib import Path
from config import REPORT_PATH


log = logging.getLogger("excel_report")


# =====================================================
#  Удаление временных директорий
# =====================================================

def cleanup_temp_dirs():
    paths = [Path("temp_extract"), Path("temp_db")]
    for p in paths:
        if p.exists():
            try:
                shutil.rmtree(p, ignore_errors=True)
                log.info(f"Удалена временная директория: {p}")
            except Exception as e:
                log.warning(f"Не удалось удалить директорию {p}: {e}")


# =====================================================
#  Убираем timezone из datetime (Excel не поддерживает TZ)
# =====================================================

def strip_tz(df: pd.DataFrame):
    """Удаляет TZ из datetime колонок, чтобы Excel мог записать значения."""
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
    """Записывает DataFrame в Excel с автошириной и удалением TZ."""
    df = strip_tz(df.copy())

    df.to_excel(writer, sheet_name=sheet_name, index=False)
    worksheet = writer.sheets[sheet_name]

    # автоширина колонок
    for idx, col in enumerate(df.columns):
        try:
            max_len = max(
                len(str(col)),
                df[col].astype(str).map(len).max() if not df.empty else len(str(col))
            )
        except Exception:
            max_len = len(str(col))
        worksheet.set_column(idx, idx, max_len + 2)


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
    - Repo Info
    - Roles (AD → Репозитории)
    - Repo Stats (обращения)
    - Users by Repo
    - Normal Users
    - Anonymous Users
    - Sessions
    """

    log.info("Начинаем формирование Excel отчёта")

    # ---------------------------------------------
    # Подготовка данных
    # ---------------------------------------------

    df_repo_sizes = pd.DataFrame([
        {"repository": repo, "size_bytes": size}
        for repo, size in repo_sizes.items()
    ])

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
    # Формирование Excel
    # ---------------------------------------------
    try:
        with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:

            write_sheet(writer, "Repo Sizes", df_repo_sizes)
            write_sheet(writer, "Repo Info", df_repo_info)
            write_sheet(writer, "Roles", df_roles)

            write_sheet(writer, "Repo Stats", df_repo_stats)
            write_sheet(writer, "Users by Repo", df_users_by_repo)

            write_sheet(writer, "Normal Users", df_normal_users)
            write_sheet(writer, "Anonymous Users", df_anon_users)
            write_sheet(writer, "Sessions", df_sessions)

        log.info(f"Excel отчёт успешно создан: {output_file}")

    except Exception as e:
        log.error(f"Ошибка при создании Excel отчёта: {e}")
        raise

    # ---------------------------------------------
    # Очистка временных директорий
    # ---------------------------------------------
    cleanup_temp_dirs()
    log.info("Очистка временных директорий завершена")
