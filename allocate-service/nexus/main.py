import logging

from config import ARCHIVE_PATH, REPORT_PATH
from log_loader import load_all_audit_logs
from log_filter import analyze_logs
from nexus_api import (
    get_roles,
    extract_ad_group_repo_mapping,
    get_repository_sizes
)
from nexus_ldap import (
    fetch_ldap_group_members,
    aggregate_users_by_groups
)
from excel_report import build_full_excel_report


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

log = logging.getLogger("main")


def main():

    # ============================================================
    # 1. Анализ логов
    # ============================================================

    log.info("=== Шаг 1: Загрузка логов → SQLite ===")
    db_path = load_all_audit_logs(ARCHIVE_PATH)

    log.info("=== Шаг 2: Анализ логов ===")
    log_stats = analyze_logs(str(db_path))


    # ============================================================
    # 2. Nexus API → AD group → repositories → sizes
    # ============================================================

    log.info("=== Шаг 3: Получаем роли из Nexus ===")
    roles = get_roles()

    log.info("=== Шаг 4: Извлекаем default AD-группы, исключаем nx-admin*, nx-anonymous* ===")
    ad_repo_map = extract_ad_group_repo_mapping(roles)

    log.info("=== Шаг 5: Получаем размеры репозиториев ===")
    repo_sizes = get_repository_sizes()


    # ============================================================
    # 3. LDAP → пользователи AD-групп
    # ============================================================

    log.info("=== Шаг 6: LDAP: получаем пользователей AD-групп ===")

    ad_groups = sorted({m["ad_group"] for m in ad_repo_map})
    log.info(f"AD-группы для LDAP: {len(ad_groups)}")

    ad_group_members = fetch_ldap_group_members(ad_groups)

    log.info("=== Шаг 7: Аггрегируем пользователей по AD-группам ===")
    users_with_groups = aggregate_users_by_groups(ad_group_members)


    # ============================================================
    # 4. Формирование Excel
    # ============================================================

    log.info("=== Шаг 8: Генерация Excel-отчёта ===")

    build_full_excel_report(
        log_stats=log_stats,
        ad_repo_map=ad_repo_map,
        repo_sizes=repo_sizes,
        users_with_groups=users_with_groups,
        output_file=REPORT_PATH,
        db_path=str(db_path)
    )

    log.info("=== Готово ===")


if __name__ == "__main__":
    main()
