import logging
import sqlite3
import re

logger = logging.getLogger("bk_users")


def load_bk_table():
    logger.info("Загружаем BK SQLite таблицу Users...")

    conn = sqlite3.connect("bk.sqlite")
    conn.row_factory = sqlite3.Row

    rows = conn.execute("SELECT * FROM Users").fetchall()
    conn.close()

    logger.info(f"Количество записей BK: {len(rows)}")

    return [dict(r) for r in rows]


# -----------------------------
# Проверка на техническую учётку
# -----------------------------

def is_cyrillic(s: str) -> bool:
    return bool(re.search(r"[а-яА-Я]", s))


def is_tech_login(login: str) -> bool:
    """
    Техническая учётка обычно:
    - только латиница + цифры + дефисы
    - без пробелов
    """
    if not login:
        return False

    if " " in login:
        return False

    return bool(re.fullmatch(r"[a-zA-Z0-9\-_.]+", login))


def classify_tech_account(ad_user: dict) -> bool:
    """
    Новая строгая логика:

    Техническая учётка, если:

    1. Нет email
    2. DisplayName содержит кириллицу, а login — машинный
    """

    email = (ad_user.get("mail") or "").strip().lower()
    display = (ad_user.get("displayName") or "")
    login = ad_user.get("ad_user") or ""

    if not email:
        return True

    if is_cyrillic(display) and is_tech_login(login):
        return True

    return False


# -----------------------------
# Основной процесс сопоставления
# -----------------------------

def match_bk_users(users_with_groups):
    bk_users = load_bk_table()

    # Хэш по email для BK
    bk_by_email = {
        (u.get("Email") or "").strip().lower(): u
        for u in bk_users
        if u.get("Email")
    }

    matched = []
    tech_accounts = []
    not_found = []

    logger.info("=== Начинаем сопоставление AD Users → BK Users ===")

    for ad_user in users_with_groups:
        email = (ad_user.get("mail") or "").strip().lower()
        ad_login = ad_user.get("ad_user")

        # 1. Техническая учётка
        if classify_tech_account(ad_user):
            logger.info(f"Тех учётка: {ad_login}")
            tech_accounts.append({**ad_user})
            continue

        # 2. Человек без email (редко, но возможно)
        if not email:
            logger.info(f"Нет email (не техучётка): {ad_login}")
            not_found.append({**ad_user})
            continue

        # 3. Сопоставляем с BK
        if email in bk_by_email:
            logger.info(f"✔ Найден в BK: {email}")
            merged = {**ad_user, **bk_by_email[email]}
            matched.append(merged)
        else:
            logger.info(f"❌ НЕ найден в BK (уволен?) → {email}")
            not_found.append({**ad_user})

    logger.info(
        f"ИТОГО: найдено = {len(matched)}, "
        f"тех учётки = {len(tech_accounts)}, "
        f"не найдено = {len(not_found)}"
    )

    return matched, tech_accounts, not_found
