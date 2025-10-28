import gitlab
import os
from dotenv import load_dotenv
import urllib3
import xlsxwriter
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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
def get_users(gl: gitlab.Gitlab) -> List[Dict[str, Any]]:
    """Возвращает список пользователей GitLab"""
    users = gl.users.list(all=True, iterator=True)
    return [
        {
            "id": u.id,
            "username": u.username,
            "email": getattr(u, "email", ""),
            "name": u.name,
            "last_sign_in_at": getattr(u, "last_sign_in_at", ""),
            "last_activity_on": getattr(u, "last_activity_on", ""),
            "identities": ", ".join(map(str, getattr(u, "identities", []) or []))
        }
        for u in users
    ]


# ======================
# 📊 Получение статистики
# ======================
def flatten_object(obj: Any, prefix: str = "") -> Dict[str, Any]:
    """Рекурсивное преобразование объекта в словарь простых типов"""
    result = {}
    if isinstance(obj, (int, float, str, bool, type(None))):
        return {prefix.strip("_"): obj}

    if hasattr(obj, "__dict__"):
        for k, v in vars(obj).items():
            result.update(flatten_object(v, f"{prefix}_{k}"))
    elif isinstance(obj, dict):
        for k, v in obj.items():
            result.update(flatten_object(v, f"{prefix}_{k}"))
    return result


def get_stat(gl: gitlab.Gitlab) -> Dict[str, Any]:
    """Получает статистику GitLab в виде простого словаря"""
    stats = gl.statistics.get()
    return flatten_object(stats)


# ======================
# 📘 Работа с Excel
# ======================
def write_to_excel(users: List[Dict[str, Any]], stats: Dict[str, Any], filename: str = None) -> str:
    """Создаёт Excel-отчёт"""
    filename = filename or f"gitlab_report_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    filename = str(Path(filename).resolve())

    workbook = xlsxwriter.Workbook(filename)
    header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
    cell_fmt = workbook.add_format({'border': 1})

    # --- Пользователи ---
    sheet_users = workbook.add_worksheet("Пользователи")
    headers = ["ID", "Username", "Email", "Name", "Last Sign In", "Last Activity", "Identities"]

    for col, header in enumerate(headers):
        sheet_users.write(0, col, header, header_fmt)

    for row, user in enumerate(users, start=1):
        for col, key in enumerate(["id", "username", "email", "name", "last_sign_in_at", "last_activity_on", "identities"]):
            sheet_users.write(row, col, user.get(key, ""), cell_fmt)

    for col in range(len(headers)):
        sheet_users.set_column(col, col, 20)

    # --- Статистика ---
    sheet_stats = workbook.add_worksheet("Статистика")
    sheet_stats.write(0, 0, "Показатель", header_fmt)
    sheet_stats.write(0, 1, "Значение", header_fmt)

    for row, (key, value) in enumerate(stats.items(), start=1):
        sheet_stats.write(row, 0, key.replace("_", " ").title(), cell_fmt)
        sheet_stats.write(row, 1, str(value), cell_fmt)

    sheet_stats.set_column(0, 0, 40)
    sheet_stats.set_column(1, 1, 30)

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
        users = get_users(gl)
        print(f"Найдено пользователей: {len(users)}")

        print("📈 Получаем статистику...")
        stats = get_stat(gl)
        print(f"Показателей статистики: {len(stats)}")

        write_to_excel(users, stats)
    except Exception as e:
        print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    main()
