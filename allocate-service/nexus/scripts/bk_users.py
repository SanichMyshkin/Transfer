import logging
import sqlite3
import re
from credentials.config import BK_SQLITE_PATH


logger = logging.getLogger("bk_users")


def load_bk_table():
    logger.info("Загружаем BK SQLite таблицу Users...")

    conn = sqlite3.connect(BK_SQLITE_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("SELECT * FROM bk").fetchall()
    conn.close()

    logger.info(f"Количество записей BK: {len(rows)}")

    return [dict(r) for r in rows]


def is_cyrillic(s: str) -> bool:
    return bool(re.search(r"[а-яА-Я]", s))


def is_full_name(display: str) -> bool:
    if not display:
        return False

    if not is_cyrillic(display):
        return False

    parts = display.strip().split()
    return len(parts) >= 2


def is_machine_like_name(s: str) -> bool:
    if not s:
        return False
    if is_cyrillic(s):
        return False

    return " " not in s


def classify_tech_account(ad_user: dict) -> bool:
    email = (ad_user.get("mail") or "").strip().lower()
    display = (ad_user.get("displayName") or "").strip()
    login = (ad_user.get("ad_user") or "").strip()

    # 1. ФИО → точно НЕ тех учётка
    if is_full_name(display):
        return False

    # 2. Нет email + display НЕ кириллица → техучётка
    if not email and not is_cyrillic(display):
        return True

    # 3. DisplayName технический (не кириллица, не имя, без пробелов)
    if is_machine_like_name(display):
        return True

    if not display and not is_cyrillic(login):
        return True

    return False


def match_bk_users(users_with_groups):
    bk_users = load_bk_table()
    bk_by_email = {
        (u.get("Email") or "").strip().lower(): u for u in bk_users if u.get("Email")
    }

    matched = []
    tech_accounts = []
    not_found = []

    logger.info("=== Начинаем сопоставление AD Users → BK Users ===")

    for ad_user in users_with_groups:
        email = (ad_user.get("mail") or "").strip().lower()
        ad_login = ad_user.get("ad_user")

        if classify_tech_account(ad_user):
            logger.info(f"Тех учётка: {ad_login}")
            tech_accounts.append({**ad_user})
            continue

        if not email:
            logger.info(f"Нет email (не техучётка): {ad_login}")
            not_found.append({**ad_user})
            continue

        if email in bk_by_email:
            logger.info(f"✔ Найден в BK: {email}")
            merged = {**ad_user, **bk_by_email[email]}
            matched.append(merged)
        else:
            logger.info(f"❌ НЕ найден в BK → {email}")
            not_found.append({**ad_user})

    logger.info(
        f"ИТОГО: найдено = {len(matched)}, "
        f"тех учётки = {len(tech_accounts)}, "
        f"не найдено = {len(not_found)}"
    )

    return matched, tech_accounts, not_found
