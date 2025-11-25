import os
import logging
import requests
import xlsxwriter
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


SONAR_URL = os.getenv("SONAR_URL")
TOKEN = os.getenv("SONAR_TOKEN")


if not SONAR_URL or not TOKEN:
    logger.error("Не заданы переменные окружения SONAR_URL и SONAR_TOKEN")
    raise SystemExit(1)


def get_sonar_users(sonar_url, token, page_size=500):
    users = []
    page = 1

    session = requests.Session()
    session.auth = (token, "")

    while True:
        params = {"p": page, "ps": page_size}
        url = f"{sonar_url}/api/users/search"

        logger.info(f"Запрос: GET {url} params={params}")

        resp = session.get(url, params=params, verify=False)
        logger.info(f"Ответ: status={resp.status_code}")

        resp.raise_for_status()
        data = resp.json()

        page_users = data.get("users", [])
        total = data.get("paging", {}).get("total", 0)

        logger.info(
            f"Получено {len(page_users)} пользователей | страница {page}, всего {total}"
        )

        users.extend(page_users)

        if page * page_size >= total:
            break

        page += 1

    return users


def write_to_excel(users, filename="sonar_users.xlsx"):
    logger.info(f"Создание Excel файла: {filename}")

    workbook = xlsxwriter.Workbook(filename)
    worksheet = workbook.add_worksheet("users")

    headers = [
        "login",
        "name",
        "email",
        "active",
        "groups",
        "tokensCount",
        "local",
        "externalIdentity",
        "externalProvider",
        "avatar",
        "lastConnectionDate",
        "managed",
    ]

    for col, header in enumerate(headers):
        worksheet.write(0, col, header)

    for row, user in enumerate(users, start=1):
        worksheet.write(row, 0, user.get("login"))
        worksheet.write(row, 1, user.get("name") or user.get("fullName"))
        worksheet.write(row, 2, user.get("email"))
        worksheet.write(row, 3, user.get("active"))
        worksheet.write(row, 4, "; ".join(user.get("groups", [])))
        worksheet.write(row, 5, user.get("tokensCount"))
        worksheet.write(row, 6, user.get("local"))
        worksheet.write(row, 7, user.get("externalIdentity"))
        worksheet.write(row, 8, user.get("externalProvider"))
        worksheet.write(row, 9, user.get("avatar"))
        worksheet.write(row, 10, user.get("lastConnectionDate"))
        worksheet.write(row, 11, user.get("managed"))

    workbook.close()
    logger.info("Excel сохранён")


def main():
    logger.info("Загружаю пользователей SonarQube")

    users = get_sonar_users(SONAR_URL, TOKEN)

    logger.info(f"Итог: пользователей получено {len(users)}")

    write_to_excel(users)


if __name__ == "__main__":
    main()
