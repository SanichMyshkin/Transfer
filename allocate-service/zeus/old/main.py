# main.py
import logging
import urllib3
from gitlab_part import gitlab_connect, get_group_projects, process_projects
from excel_writer import write_excel

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=== Запуск GitLab + LDAP интеграции ===")
    gl = gitlab_connect()
    logger.info("Получаем проекты группы...")
    projects = get_group_projects(gl)
    logger.info(f"Найдено проектов: {len(projects)}")
    data = process_projects(gl, projects)
    logger.info(f"Готово. Проектов с мониторинг-файлами: {len(data)}")
    write_excel(data)
    logger.info("Excel отчет создан: gitlab_report.xlsx")
    logger.info("=== Готово ===")


if __name__ == "__main__":
    main()
