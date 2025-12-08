import logging
from log_loader import load_all_audit_logs
from log_filter import process_logs
from nexus_api import (
    get_repository_sizes,
    get_repository_data,
    get_roles,
    get_ad_groups_from_roles,
    map_roles_to_repositories,
)
from excel_report import build_excel_report
from config import REPORT_PATH

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

log = logging.getLogger("main")


def main():
    log.info("🚀 Старт обработки данных Nexus")

    # ======================================================
    # 1. Загружаем архив с логами → SQLite
    # ======================================================
    sqlite_path = load_all_audit_logs("path/to/archive.zip")
    log.info(f"SQLite база логов: {sqlite_path}")

    # ======================================================
    # 2. Фильтруем и анализируем логи
    # ======================================================
    log_stats = process_logs(sqlite_path)
    log.info("Анализ логов завершён")

    # ======================================================
    # 3. Данные о репозиториях из PostgreSQL
    # ======================================================
    repo_sizes = get_repository_sizes()
    repo_data = get_repository_data()
    log.info("Получены данные о репозиториях")

    # ======================================================
    # 4. Роли Nexus (REST API)
    # ======================================================
    roles = get_roles()
    ad_map = get_ad_groups_from_roles(roles)
    role_repo_map = map_roles_to_repositories(roles)
    log.info("Получены роли Nexus и их связи с репозиториями")

    # ======================================================
    # 5. Генерация Excel
    # ======================================================
    build_excel_report(
        repo_sizes=repo_sizes,
        repo_data=repo_data,
        role_repo_map=role_repo_map,
        ad_map=ad_map,
        log_stats=log_stats,
        output_file=REPORT_PATH,
    )

    log.info(f"Отчёт готов: {REPORT_PATH}")


if __name__ == "__main__":
    main()
