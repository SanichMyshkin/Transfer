import gitlab
import os
import logging
from dotenv import load_dotenv
import urllib3
import xlsxwriter
from pathlib import Path
import time

# ======================
# ⚙️ Настройки окружения
# ======================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")

LOG_FILE = "gitlab_report.log"

# ======================
# 🧠 Логирование
# ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
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
# 👥 Пользователи
# ======================
def get_users(gl: gitlab.Gitlab):
    logger.info("Получаем пользователей...")
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
# 📁 Проекты с детализацией
# ======================
def get_projects_stats(gl: gitlab.Gitlab):
    logger.info("Начинаем сбор статистики по проектам...")
    projects = gl.projects.list(all=True, iterator=True)
    result = []
    total_commits = 0

    for idx, project in enumerate(projects, start=1):
        try:
            # Получаем полную информацию по каждому проекту
            full_proj = gl.projects.get(project.id, statistics=True)
            stats = getattr(full_proj, "statistics", {}) or {}

            commit_count = stats.get("commit_count", 0)
            if isinstance(commit_count, int):
                total_commits += commit_count

            project_data = {
                "id": full_proj.id,
                "name": full_proj.name,
                "path_with_namespace": full_proj.path_with_namespace,
                "repository_size_mb": round(stats.get("repository_size", 0) / 1024 / 1024, 2),
                "lfs_objects_size_mb": round(stats.get("lfs_objects_size", 0) / 1024 / 1024, 2),
                "job_artifacts_size_mb": round(stats.get("job_artifacts_size", 0) / 1024 / 1024, 2),
                "storage_size_mb": round(stats.get("storage_size", 0) / 1024 / 1024, 2),
                "commit_count": commit_count,
                "last_activity_at": full_proj.last_activity_at,
                "visibility": full_proj.visibility,
            }

            result.append(project_data)

            if idx % 50 == 0:
                logger.info(f"Обработано проектов: {idx}")

            time.sleep(0.05)

        except Exception as e:
            logger.warning(f"Ошибка при обработке проекта {getattr(project, 'path_with_namespace', project.id)}: {e}")
            continue

    result.sort(key=lambda x: x.get("storage_size_mb", 0), reverse=True)

    logger.info(f"✅ Сбор статистики завершён: всего проектов — {len(result)}, всего коммитов — {total_commits}")
    return result, total_commits


# ======================
# 📘 Запись отчёта
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

    # --- Общая статистика ---
    stats_sheet = workbook.add_worksheet("Статистика")
    stats_sheet.write(0, 0, "Показатель", header_format)
    stats_sheet.write(0, 1, "Значение", header_format)

    for row, (key, value) in enumerate(statistics_data.items(), start=1):
        stats_sheet.write(row, 0, key.replace("_", " ").title(), cell_format)
        stats_sheet.write(row, 1, value, cell_format)

    # --- Проекты ---
    projects_sheet = workbook.add_worksheet("Проекты")
    proj_headers = [
        "ID", "Project Name", "Namespace Path",
        "Repo Size (MB)", "LFS Size (MB)",
        "Artifacts Size (MB)", "Total Storage (MB)",
        "Commits", "Last Activity", "Visibility"
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
        projects_sheet.write(row, 8, p["last_activity_at"], cell_format)
        projects_sheet.write(row, 9, p["visibility"], cell_format)

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
        projects_data, total_commits = get_projects_stats(gl)

        statistics_data["total_commits"] = total_commits  # добавляем общие коммиты

        write_to_excel(users_data, statistics_data, projects_data)
        logger.info("✅ Работа успешно завершена.\n")

    except Exception as e:
        logger.exception(f"❌ Ошибка выполнения: {e}")


if __name__ == "__main__":
    main()
