import shutil
import logging
import pandas as pd
from pathlib import Path
from config import REPORT_PATH

log = logging.getLogger("excel_report")


# ============================================================
#  Очистка временных директорий
# ============================================================

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
#  Удаление TZ (Excel не поддерживает)
# ============================================================

def strip_tz(df: pd.DataFrame):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = df[col].dt.tz_localize(None)
            except Exception:
                pass
    return df


# ============================================================
#  Универсальная запись листа
# ============================================================

def write_sheet(writer, sheet_name: str, df: pd.DataFrame):
    df = strip_tz(df.copy())
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    ws = writer.sheets[sheet_name]

    # Автоширина
    for idx, col in enumerate(df.columns):
        try:
            max_len = max(
                len(str(col)),
                df[col].astype(str).map(len).max() if len(df) else len(str(col))
            )
        except:
            max_len = len(str(col))
        ws.set_column(idx, idx, max_len + 2)


# ============================================================
#  Основная функция формирования отчёта
# ============================================================

def build_excel_report(
    repo_sizes,
    log_stats,
    ad_group_repo_map,
    output_file=REPORT_PATH
):
    log.info("Формируем Excel отчёт")

    # --------------------------------------------------------
    # AD Repo Usage (объединённый лист)
    # --------------------------------------------------------
    rows = []

    for mapping in ad_group_repo_map:
        ad = mapping["ad_group"]
        repo = mapping["repository"]

        size_info = repo_sizes.get(repo, {"size_human": "0 B"})

        rows.append({
            "ad_group": ad,
            "repository": repo,
            "size": size_info["size_human"]
        })

    df_ad_repo_usage = pd.DataFrame(rows)

    # --------------------------------------------------------
    # Остальные листы из логов
    # --------------------------------------------------------

    df_repo_stats     = strip_tz(log_stats["repo_stats"])
    df_users_by_repo  = strip_tz(log_stats["users_by_repo"])
    df_normal_users   = strip_tz(log_stats["normal_users"])
    df_anonymous      = strip_tz(log_stats["anonymous_users"])

    # --------------------------------------------------------
    # Записываем Excel
    # --------------------------------------------------------

    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:

        write_sheet(writer, "AD Repo Usage", df_ad_repo_usage)
        write_sheet(writer, "Repo Stats", df_repo_stats)
        write_sheet(writer, "Users by Repo", df_users_by_repo)
        write_sheet(writer, "Normal Users", df_normal_users)
        write_sheet(writer, "Anonymous Users", df_anonymous)

    log.info(f"Отчёт создан: {output_file}")

    cleanup_temp_dirs()
    log.info("Очищены временные директории")
