import logging
from log_loader import load_all_audit_logs
from log_filter import process_logs
from nexus_api import get_repository_sizes, get_roles, extract_ad_group_repo_mapping
from excel_report import build_excel_report
from config import REPORT_PATH


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

log = logging.getLogger("main")


def main():
    log.info("Запуск анализа Nexus...")

    # ============================================================
    # 1. Загрузка логов
    # ============================================================
    archive_path = "path/to/archive.zip"  # заменить
    sqlite_path = load_all_audit_logs(archive_path)
    log.info(f"SQLite база логов: {sqlite_path}")

    # ============================================================
    # 2. Фильтрация логов
    # ============================================================
    log_stats = process_logs(sqlite_path)
    log.info("Логи обработаны")

    # ============================================================
    # 3. Получение размеров репозиториев
    # ============================================================
    repo_sizes = get_repository_sizes()
    log.info("Размеры репозиториев получены")

    # ============================================================
    # 4. Получение AD-групп (default roles)
    # ============================================================
    roles = get_roles()
    ad_group_repo_map = extract_ad_group_repo_mapping(roles)

    # ============================================================
    # 5. Генерация Excel
    # ============================================================
    build_excel_report(
        repo_sizes=repo_sizes,
        log_stats=log_stats,
        ad_group_repo_map=ad_group_repo_map,
        output_file=REPORT_PATH,
    )

    log.info(f"Отчёт готов: {REPORT_PATH}")
    log.info("Готово!")


if __name__ == "__main__":
    main()
