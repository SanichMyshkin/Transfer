import logging
from log_loader import load_all_audit_logs
from log_filter import process_logs

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

log = logging.getLogger("main")


def main():
    log.info("Старт обработки логов")

    sqlite_path = load_all_audit_logs("PATH_TO_ARCHIVE.zip")
    log.info(f"SQLite создан: {sqlite_path}")

    stats = process_logs(sqlite_path)

    log.info("Готово. Данные собраны.")

    # пример использования:
    print(stats["repo_stats"])
    print(stats["users_by_repo"].head())


if __name__ == "__main__":
    main()
