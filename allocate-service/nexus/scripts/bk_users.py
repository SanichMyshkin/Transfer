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
TECH_EMAIL_PATTERNS = [r"robot", r"bot", r"service", r"svc", r"automation", r"system"]


def is_tech_email(email: str) -> bool:
    if not email:
        return False
    low = email.lower()
    return any(pat in low for pat in TECH_EMAIL_PATTERNS)


def is_cyrillic(s: str) -> bool:
    return bool(re.search(r"[а-яА-Я]", s))


def is_tech_login(login: str) -> bool:
    """
    Техническая учётка обычно:
    - только латиница + цифры + дефисы
    - без пробелов
    - цельным словом (или через дефисы)
    """
    if not login:
        return False

    if " " in login:
        return False

    return bool(re.fullmatch(r"[a-zA-Z0-9\-_.]+", login))


def classify_tech_account(ad_user: dict) -> bool:
    """
    Основная логика:
    - если нет email → тех учётка
    - если email технический → тех учётка
    - если displayName содержит кириллицу, а логин выглядит техническим → тех учётка
    """
    email = (ad_user.get("mail") or "").strip().lower()
    display = ad_user.get("displayName") or ""
    login = ad_user.get("ad_user") or ""

    if not email:
        return True

    if is_tech_email(email):
        return True

    if is_cyrillic(display) and is_tech_login(login):
        return True

    return False


# -----------------------------
# Основной процесс сопоставления
# -----------------------------
def match_bk_users(users_with_groups):
    bk_users = load_bk_table()

    # Хэш: email → BK запись
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

        # 1. Техническая учётка?
        if classify_tech_account(ad_user):
            logger.info(f"Тех учётка: {ad_login}")
            tech_accounts.append({**ad_user})
            continue

        # 2. Нормальный пользователь, но нет email
        if not email:
            logger.info(
                f"Нет email, но логика классифицировала НЕ как тех учётку → {ad_login}"
            )
            not_found.append({**ad_user})
            continue

        # 3. Есть email → ищем в BK
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
