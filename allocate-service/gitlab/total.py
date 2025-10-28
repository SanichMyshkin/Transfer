import gitlab
import os
from dotenv import load_dotenv
import urllib3
import xlsxwriter
from pathlib import Path

# Отключаем предупреждения SSL (если GitLab с самоподписанным сертификатом)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Загружаем переменные окружения
load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")


# ======================
# 🔗 Подключение к GitLab
# ======================
def get_gitlab_connection(url: str, token: str) -> gitlab.Gitlab:
    """Создаёт авторизованное соединение с GitLab"""
    gl = gitlab.Gitlab(url, private_token=token, ssl_verify=False, timeout=30)
    gl.auth()
    return gl


# ======================
# 👥 Получение пользователей
# ======================
def get_users(gl: gitlab.Gitlab):
    """Возвращает список пользователей GitLab"""
    users = gl.users.list(all=True, iterator=True)
    result = []

    for u in users:
        # Извлекаем extern_uid из identities, если есть
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

    return result


# ======================
# 📊 Получение статистики
# ======================
def get_stat(gl: gitlab.Gitlab):
    """Получение общей статистики GitLab (всё плоско, без рекурсии)"""
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

    # Преобразуем строковые числа в int, если это возможно
    for k, v in stats_dict.items():
        if isinstance(v, str):
            value = v.replace(",", "").strip()
            if value.isdigit():
                stats_dict[k] = int(value)

    return stats_dict


# ======================
# 📘 Запись данных в Excel
# ======================
def write_to_excel(users_data, statistics_data, filename="gitlab_report.xlsx"):
    """Создание Excel-отчёта"""
    filename = str(Path(filename).resolve())
    workbook = xlsxwriter.Workbook(filename)

    header_format = workbook.add_format(
        {"bold": True, "bg_color": "#D3D3D3", "border": 1}
    )
    cell_format = workbook.add_format({"border": 1})

    # --- Лист с пользователями ---
    users_sheet = workbook.add_worksheet("Пользователи")
    user_headers = [
        "ID",
        "Username",
        "Email",
        "Name",
        "Last Sign In",
        "Last Activity",
        "Extern UID",
    ]

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

    for col in range(len(user_headers)):
        users_sheet.set_column(col, col, 20)

    # --- Лист со статистикой ---
    stats_sheet = workbook.add_worksheet("Статистика")
    stats_sheet.write(0, 0, "Показатель", header_format)
    stats_sheet.write(0, 1, "Значение", header_format)

    for row, (key, value) in enumerate(statistics_data.items(), start=1):
        stats_sheet.write(row, 0, key.replace("_", " ").title(), cell_format)
        stats_sheet.write(row, 1, value, cell_format)

    stats_sheet.set_column(0, 0, 30)
    stats_sheet.set_column(1, 1, 20)

    workbook.close()
    print(f"✅ Отчёт сохранён: {filename}")
    return filename


# ======================
# 🚀 Основной запуск
# ======================
def main():
    try:
        gl = get_gitlab_connection(GITLAB_URL, GITLAB_TOKEN)
        print("🔗 Успешное подключение к GitLab")

        print("📥 Получаем пользователей...")
        users_data = get_users(gl)
        print(f"Найдено пользователей: {len(users_data)}")

        print("📈 Получаем статистику...")
        statistics_data = get_stat(gl)
        print("Статистика GitLab:")
        for k, v in statistics_data.items():
            print(f"  {k}: {v}")

        write_to_excel(users_data, statistics_data)

    except Exception as e:
        print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    main()
