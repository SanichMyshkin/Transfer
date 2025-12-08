import logging

from config import ARCHIVE_PATH, REPORT_PATH
from log_loader import load_all_audit_logs
from log_filter import analyze_logs
from nexus_api import get_repository_sizes, get_roles, extract_ad_group_repo_mapping
from excel_report import build_full_report


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

log = logging.getLogger("main")


def main():
    log.info("Старт анализа Nexus")

    log.info(f"Архив с логами: {ARCHIVE_PATH}")
    db_path = load_all_audit_logs(ARCHIVE_PATH)
    log.info(f"SQLite база: {db_path}")

    stats = analyze_logs(str(db_path))

    repo_sizes = get_repository_sizes()

    roles = get_roles()
    ad_group_repo_map = extract_ad_group_repo_mapping(roles)

    build_full_report(
        stats=stats,
        repo_sizes=repo_sizes,
        ad_group_repo_map=ad_group_repo_map,
        output_file=REPORT_PATH,
        db_path=str(db_path),
    )

    log.info(f"Отчёт сформирован: {REPORT_PATH}")


if __name__ == "__main__":
    main()
