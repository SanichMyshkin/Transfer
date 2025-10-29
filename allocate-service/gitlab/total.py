import gitlab
import os
import logging
from dotenv import load_dotenv
import urllib3
import xlsxwriter
from pathlib import Path

# ======================
# ⚙️ Настройки окружения
# ======================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")

# ======================
# 🧠 Настройка логирования
# ======================
LOG_FILE = "gitlab_report.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),  # ⬅️ дозапись
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ======================
# 🔗 Подключение к GitLab
# ======================
def get_gitlab_connection(url: str, token: str) -> gitlab.Gitlab:
    logger.info("Подключаемся к GitLab...")
    gl = gitlab.Gitlab(url, private_token=token, ssl_verify=False, timeout=60)
    gl.auth()
    logger.info("Успешное подключение к GitLab")
    return gl


# ======================
# 👥 Получение пользователей
# ======================
def get_users(gl: gitlab.Gitlab):
    logger.info("Получаем список пользователей...")
    users = gl.users.list(all=True, iterator=True)
    result = []

    for u in users:
        extern_uid = ""
        identities = getattr(u, "identities", [])
        if identities and isinstance(identities, list):
            extern_uid = ", ".join(
                i.get("extern_uid", "") for i in identities if isinstance(i, dict)
            )

        result.append(
            {
                "id": u.id,
                "username": u.username,
                "email": getattr(u, "email", ""),
                "name": u.name,
                "last_sign_in_at": getattr(u, "last_sign_in_at", ""),
                "last_activity_on": getattr(u, "last_activity_on", ""),
                "extern_uid": extern_uid,
            }
        )

    logger.info(f"Пользователей получено: {len(result)}")
    return result


# ======================
# 📊 Общая статистика GitLab
# ======================
def get_stat(gl: gitlab.Gitlab):
    logger.info("Получаем общую статистику GitLab...")
    stats = gl.statistics.get()

    stats_dict = {
        "forks": stats.forks,
        "issues": stats.issues,
        "merge_requests": stats.merge_requests,
        "notes": stats.notes,
        "snippets": stats.snippets,
        "ssh_keys": stats.ssh_keys,
        "milestones": stats.milestones,
        "users": stats.users,
        "projects": stats.projects,
        "groups": stats.groups,
        "active_users": stats.active_users,
    }

    for k, v in stats_dict.items():
        if isinstance(v, str):
            value = v.replace(",", "").strip()
            if value.isdigit():
                stats_dict[k] = int(value)

    logger.info("Общая статистика успешно получена.")
    return stats_dict


# ======================
# 📁 Статистика по проектам
# ======================
def get_projects_stats(gl: gitlab.Gitlab):
    logger.info("Начинаем сбор статистики по проектам...")
    projects = gl.projects.list(all=True, iterator=True)
    result = []

    for idx, p in enumerate(projects, start=1):
        try:
            logger.info(f"[{idx}] Обработка проекта: {p.path_with_namespace}")
            project = gl.projects.get(p.id, statistics=True)
            stats = getattr(project, "statistics", {}) or {}

            # Кол-во коммитов
            try:
                commits_count = len(project.commits.list(all=True))
            except Exception:
                commits_count = "N/A"

            # Кол-во веток
            try:
                branches_count = len(project.branches.list(all=True))
            except Exception:
                branches_count = "N/A"

            # Кол-во тегов
            try:
                tags_count = len(project.tags.list(all=True))
            except Exception:
                tags_count = "N/A"

            result.append({
                "id": project.id,
                "name": project.name,
                "path_with_namespace": project.path_with_namespace,
                "repository_size_mb": round(stats.get("repository_size", 0) / 1024 / 1024, 2),
                "lfs_objects_size_mb": round(stats.get("lfs_objects_size", 0) / 1024 / 1024, 2),
                "job_artifacts_size_mb": round(stats.get("job_artifacts_size", 0) / 1024 / 1024, 2),
                "storage_size_mb": round(stats.get("storage_size", 0) / 1024 / 1024, 2),
                "commit_count": commits_count,
                "branches_count": branches_count,
                "tags_count": tags_count,
                "last_activity_at": project.last_activity_at,
                "visibility": project.visibility,
            })
        except Exception as e:
            logger.warning(f"Ошибка при обработке проекта {p.path_with_namespace}: {e}")
            continue

    logger.info(f"Статистика собрана для {len(result)} проектов.")
    return result


# ======================
# 📘 Запись отчёта в Excel
# ======================
def write_to_excel(users_data, statistics_data, projects_data, filename="gitlab_report.xlsx"):
    filename = str(Path(filename).resolve())
    logger.info(f"Создаём Excel-отчёт: {filename}")

    workbook = xlsxwriter.Workbook(filename)
    header_format = workbook.add_format({"bold": True, "bg_color": "#D3D3D3", "border": 1})
    cell_format = workbook.add_format({"border": 1})

    # --- Пользователи ---
    users_sheet = workbook.add_worksheet("Пользователи")
    user_headers = ["ID", "Username", "Email", "Name", "Last Sign In", "Last Activity", "Extern UID"]

    for col, header in enumerate(user_headers):
        users_sheet.write(0, col, header, header_format)

    for row, user in enumerate(users_data, start=1):
        users_sheet.write(row, 0, user["id"], cell_format)
        users_sheet.write(row, 1, user["username"], cell_format)
        users_sheet.write(row, 2, user["email"], cell_format)
        users_sheet.write(row, 3, user["name"], cell_format)
        users_sheet.write(row, 4, user["last_sign_in_at"], cell_format)
        users_sheet.write(row, 5, user["last_activity_on"], cell_format)
        users_sheet.write(row, 6, user["extern_uid"], cell_format)

    users_sheet.set_column(0, len(user_headers) - 1, 20)

    # --- Общая статистика ---
    stats_sheet = workbook.add_worksheet("Статистика")
    stats_sheet.write(0, 0, "Показатель", header_format)
    stats_sheet.write(0, 1, "Значение", header_format)

    for row, (key, value) in enumerate(statistics_data.items(), start=1):
        stats_sheet.write(row, 0, key.replace("_", " ").title(), cell_format)
        stats_sheet.write(row, 1, value, cell_format)

    stats_sheet.set_column(0, 0, 30)
    stats_sheet.set_column(1, 1, 20)

    # --- Проекты ---
    projects_sheet = workbook.add_worksheet("Проекты")
    proj_headers = [
        "ID", "Project Name", "Namespace Path", "Repo Size (MB)", "LFS Size (MB)",
        "Artifacts Size (MB)", "Total Storage (MB)", "Commits", "Branches", "Tags",
        "Last Activity", "Visibility"
    ]

    for col, header in enumerate(proj_headers):
        projects_sheet.write(0, col, header, header_format)

    for row, p in enumerate(projects_data, start=1):
        projects_sheet.write(row, 0, p["id"], cell_format)
        projects_sheet.write(row, 1, p["name"], cell_format)
        projects_sheet.write(row, 2, p["path_with_namespace"], cell_format)
        projects_sheet.write(row, 3, p["repository_size_mb"], cell_format)
        projects_sheet.write(row, 4, p["lfs_objects_size_mb"], cell_format)
        projects_sheet.write(row, 5, p["job_artifacts_size_mb"], cell_format)
        projects_sheet.write(row, 6, p["storage_size_mb"], cell_format)
        projects_sheet.write(row, 7, p["commit_count"], cell_format)
        projects_sheet.write(row, 8, p["branches_count"], cell_format)
        projects_sheet.write(row, 9, p["tags_count"], cell_format)
        projects_sheet.write(row, 10, p["last_activity_at"], cell_format)
        projects_sheet.write(row, 11, p["visibility"], cell_format)

    projects_sheet.set_column(0, len(proj_headers) - 1, 20)

    workbook.close()
    logger.info(f"Отчёт успешно сохранён: {filename}")
    return filename


# ======================
# 🚀 Основной запуск
# ======================
def main():
    try:
        logger.info("========== ЗАПУСК ОТЧЁТА GitLab ==========")
        gl = get_gitlab_connection(GITLAB_URL, GITLAB_TOKEN)

        users_data = get_users(gl)
        statistics_data = get_stat(gl)
        projects_data = get_projects_stats(gl)

        write_to_excel(users_data, statistics_data, projects_data)
        logger.info("✅ Работа успешно завершена.\n")

    except Exception as e:
        logger.exception(f"❌ Ошибка выполнения: {e}")


if __name__ == "__main__":
    main()
