import shutil
import logging
import pandas as pd
from pathlib import Path
from config import REPORT_PATH


log = logging.getLogger("excel_report")


def cleanup_temp_dirs():
    temp_dirs = [Path("temp_extract"), Path("temp_db")]

    for d in temp_dirs:
        if d.exists():
            try:
                shutil.rmtree(d, ignore_errors=True)
                log.info(f"Удалена временная директория: {d}")
            except Exception as e:
                log.warning(f"Не удалось удалить {d}: {e}")


# ============================================================


def strip_tz(df: pd.DataFrame):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = df[col].dt.tz_localize(None)
            except Exception:
                pass
    return df


def write_sheet(writer, sheet_name: str, df: pd.DataFrame):
    df = strip_tz(df.copy())

    df.to_excel(writer, sheet_name=sheet_name, index=False)
    worksheet = writer.sheets[sheet_name]

    # автоширина
    for idx, col in enumerate(df.columns):
        try:
            max_len = max(
                len(str(col)),
                df[col].astype(str).map(len).max() if not df.empty else len(str(col)),
            )
        except Exception:
            max_len = len(str(col))

        worksheet.set_column(idx, idx, max_len + 2)


def build_excel_report(
    repo_sizes, log_stats, ad_group_repo_map, output_file=REPORT_PATH
):
    """
    Генерирует итоговый Excel отчёт со всеми листами:

    - Repo Sizes
    - AD Groups
    - Repo Stats
    - Users by Repo
    - Normal Users
    - Anonymous Users
    - Sessions
    """

    log.info("Начинаем формирование Excel отчёта")

    df_repo_sizes = pd.DataFrame(
        [
            {
                "repository": repo,
                "size_bytes": data["size_bytes"],
                "size_human": data["size_human"],
            }
            for repo, data in repo_sizes.items()
        ]
    )

    df_ad_groups = pd.DataFrame(ad_group_repo_map)

    df_repo_stats = strip_tz(log_stats["repo_stats"])
    df_users_by_repo = strip_tz(log_stats["users_by_repo"])
    df_normal_users = strip_tz(log_stats["normal_users"])
    df_anonymous = strip_tz(log_stats["anonymous_users"])
    df_sessions = strip_tz(log_stats["sessions"])

    try:
        with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
            write_sheet(writer, "Repo Sizes", df_repo_sizes)
            write_sheet(writer, "AD Groups", df_ad_groups)

            write_sheet(writer, "Repo Stats", df_repo_stats)
            write_sheet(writer, "Users by Repo", df_users_by_repo)

            write_sheet(writer, "Normal Users", df_normal_users)
            write_sheet(writer, "Anonymous Users", df_anonymous)

            write_sheet(writer, "Sessions", df_sessions)

        log.info(f"Excel отчёт успешно записан: {output_file}")

    except Exception as e:
        log.error(f"Ошибка при записи Excel: {e}")
        raise

    # --------------------------------------------------------
    # Очистка временных директорий
    # --------------------------------------------------------
    cleanup_temp_dirs()
    log.info("Очистка временных директорий завершена")
