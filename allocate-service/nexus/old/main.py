import logging

from credentials.config import ARCHIVE_PATH, REPORT_PATH

from scripts.log_loader import load_all_audit_logs
from scripts.log_filter import analyze_logs

from scripts.nexus_api import (
    get_roles,
    extract_group_repo_relation,
    extract_all_default_groups,
    get_repository_sizes,
)

from scripts.nexus_ldap import (
    fetch_ldap_group_members,
    aggregate_users_by_groups,
)

from scripts.bk_users import match_bk_users

from scripts.excel_report import build_full_report


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("main")


def main():
    log.info("=== Шаг 1: Загрузка логов → SQLite ===")
    db_path = load_all_audit_logs(ARCHIVE_PATH)

    log.info("=== Шаг 2: Анализ логов ===")
    log_stats = analyze_logs(str(db_path))

    log.info("=== Шаг 3: Получаем роли Nexus ===")
    roles = get_roles()

    log.info("=== Шаг 4: Строим отображение repo → ad_groups ===")
    ad_repo_map = extract_group_repo_relation(roles)

    log.info("=== Шаг 5: Собираем ВСЕ default AD-группы ===")
    ad_groups_all = extract_all_default_groups(roles)

    log.info("=== Шаг 6: Получаем размеры репозиториев ===")
    repo_sizes = get_repository_sizes()

    log.info("=== Шаг 7: LDAP: получаем пользователей всех default AD-групп ===")
    ad_group_members = fetch_ldap_group_members(ad_groups_all)

    log.info("=== Шаг 8: Агрегируем пользователей по AD-группам ===")
    users_with_groups = aggregate_users_by_groups(ad_group_members)

    log.info("=== Шаг 9: Сопоставляем BK Users по email ===")
    bk_users = match_bk_users(users_with_groups)

    log.info("=== Шаг 10: Формируем Excel отчёт ===")
    build_full_report(
        log_stats=log_stats,
        ad_repo_map=ad_repo_map,
        repo_sizes=repo_sizes,
        users_with_groups=users_with_groups,
        bk_users_tuple=bk_users,
        output_file=REPORT_PATH,
        db_path=str(db_path),
    )

    log.info("=== ГОТОВО: полный отчёт сформирован ===")
    log.info(f"Файл: {REPORT_PATH}")


if __name__ == "__main__":
    main()
