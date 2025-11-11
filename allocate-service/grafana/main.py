import requests
import pandas as pd

# === НАСТРОЙКИ ===
GRAFANA_URL = "https://grafana.sanich.tech"
API_TOKEN = 
OUTPUT_FILE = "grafana_users_report.xlsx"

HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}

# === ФУНКЦИИ ===
def get_users():
    users = []
    page = 1
    while True:
        resp = requests.get(f"{GRAFANA_URL}/api/users/search?perpage=100&page={page}", headers=HEADERS)
        if resp.status_code != 200:
            print("Ошибка при получении пользователей:", resp.text)
            break
        data = resp.json()
        if not data.get("users"):
            break
        users.extend(data["users"])
        if len(data["users"]) < 100:
            break
        page += 1
    return users

def get_teams():
    resp = requests.get(f"{GRAFANA_URL}/api/teams/search?perpage=1000", headers=HEADERS)
    if resp.status_code != 200:
        print("Ошибка при получении команд:", resp.text)
        return []
    return resp.json().get("teams", [])

def get_user_teams(user_id):
    resp = requests.get(f"{GRAFANA_URL}/api/users/{user_id}/teams", headers=HEADERS)
    if resp.status_code != 200:
        return []
    return [t["name"] for t in resp.json()]

# === ОСНОВНАЯ ЛОГИКА ===
users_data = []
users = get_users()
teams = get_teams()

for user in users:
    user_teams = ", ".join(get_user_teams(user["id"]))
    users_data.append({
        "ID": user["id"],
        "Логин": user.get("login"),
        "Имя": user.get("name"),
        "Email": user.get("email"),
        "Активен": not user.get("isDisabled", False),
        "Роль": user.get("role", "N/A"),
        "Команды": user_teams,
        "Последний вход": user.get("lastSeenAt") or "—",
        "Создан": user.get("createdAt") or "—"
    })

# === ВЫГРУЗКА В EXCEL ===
df = pd.DataFrame(users_data)
df.to_excel(OUTPUT_FILE, index=False)
print(f"Отчёт сохранён в {OUTPUT_FILE}")
