import logging
import sqlite3
import re
from credentials.config import BK_SQLITE_PATH


logger = logging.getLogger("bk_users")


# -----------------------------
# ЗАГРУЗКА BK
# -----------------------------


def load_bk_table():
    logger.info("Загружаем BK SQLite таблицу Users...")

    conn = sqlite3.connect(BK_SQLITE_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("SELECT * FROM bk").fetchall()
    conn.close()

    logger.info(f"Количество записей BK: {len(rows)}")

    return [dict(r) for r in rows]


# -----------------------------
# УТИЛИТЫ
# -----------------------------


def is_cyrillic(s: str) -> bool:
    return bool(re.search(r"[а-яА-Я]", s))


def is_full_name(display: str) -> bool:
    """
    Считаем ФИО настоящим, если:
    - минимум два слова
    - содержит кириллицу
    """
    if not display:
        return False

    parts = display.strip().split()
    if len(parts) < 2:  # Имя и Фамилия должны быть минимум
        return False

    return is_cyrillic(display)


def is_tech_login(login: str) -> bool:
    """
    Базовое правило для машинного логина.
    Но теперь он НЕ определяет техучётку сам по себе — только в совокупности.
    """
    if not login:
        return False

    if " " in login:
        return False

    return bool(re.fullmatch(r"[a-zA-Z0-9\-_.]+", login))


def is_strongly_machine_login(login: str) -> bool:
    """
    Стандартные префиксы техучёток.
    """
    if not login:
        return False

    login = login.lower()

    return bool(re.match(r"^(svc_|sys_|bot_|tech_|service_|app_|auto_|system_)", login))


# -----------------------------
# НОВАЯ ЛОГИКА ОПРЕДЕЛЕНИЯ ТЕХУЧЁТКИ
# -----------------------------


def classify_tech_account(ad_user: dict) -> bool:
    email = (ad_user.get("mail") or "").strip().lower()
    display = (ad_user.get("displayName") or "").strip()
    login = (ad_user.get("ad_user") or "").strip()

    # 1. Если есть нормальное ФИО — ЭТО ТОЧНО НЕ ТЕХУЧЁТКА
    if is_full_name(display):
        return False

    # 2. Сильные признаки техучётки
    if is_strongly_machine_login(login):
        return True

    # 3. Нет email + нет ФИО → техучётка
    if not email and not is_cyrillic(display):
        return True

    # 4. DisplayName технический (нет кириллицы) + машинный логин
    if not is_cyrillic(display) and is_tech_login(login):
        return True

    return False


# -----------------------------
# ОСНОВНОЙ ПРОЦЕСС СОПОСТАВЛЕНИЯ
# -----------------------------


def match_bk_users(users_with_groups):
    bk_users = load_bk_table()

    # Хэш по email для BK
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

        # 1. Определяем технические учётки
        if classify_tech_account(ad_user):
            logger.info(f"Тех учётка: {ad_login}")
            tech_accounts.append({**ad_user})
            continue

        # 2. Человек без email (бывает)
        if not email:
            logger.info(f"Нет email (но НЕ техучётка): {ad_login}")
            not_found.append({**ad_user})
            continue

        # 3. Пытаемся сопоставить с BK
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
