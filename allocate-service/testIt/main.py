import logging
import os
from dotenv import load_dotenv

from testit_api_client import Configuration, ApiClient
from testit_api_client.api.users_api import UsersApi

# -----------------------------------
# Setup
# -----------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

HOST = os.getenv("TESTIT_HOST")
TOKEN = os.getenv("TESTIT_TOKEN")

if not HOST or not TOKEN:
    raise ValueError("TESTIT_HOST или TESTIT_TOKEN отсутствуют в .env")

config = Configuration(host=HOST)

# -----------------------------------
# Fetch users
# -----------------------------------
def get_users():
    logging.info("Запрашиваю пользователей из Test IT...")

    with ApiClient(
        config,
        header_name="Authorization",
        header_value="PrivateToken " + TOKEN
    ) as api_client:

        api = UsersApi(api_client)

        try:
            users = api.api_v2_users_get()  # GET /api/v2/users
        except Exception as e:
            logging.error(f"Ошибка запроса пользователей: {e}")
            raise

        for u in users:
            logging.info(f"Пользователь: {u.id} | {u.login} | {u.email} | {u.display_name}")

        return users


if __name__ == "__main__":
    get_users()
