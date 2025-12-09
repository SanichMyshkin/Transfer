import logging
import sqlite3


logger = logging.getLogger("bk_users")


def load_bk_table():

    logger.info("Загружаем BK SQLite таблицу Users...")

    conn = sqlite3.connect("bk.sqlite")
    conn.row_factory = sqlite3.Row

    rows = conn.execute("SELECT * FROM Users").fetchall()
    conn.close()

    logger.info(f"Количество записей BK: {len(rows)}")

    return [dict(r) for r in rows]


def match_bk_users(users_with_groups):
    """
    Возвращает ТРИ списка:

    1. matched      — найденные в BK по email
    2. no_email     — учётки из AD без email (технические)
    3. not_found    — есть email, но нет совпадения в BK (уволенные)
    """

    bk_users = load_bk_table()

    # Хэш таблица по email из BK
    bk_by_email = {
        (u.get("Email") or "").strip().lower(): u for u in bk_users if u.get("Email")
    }

    matched = []
    no_email = []
    not_found = []

    logger.info("=== Начинаем сопоставление AD Users → BK Users ===")

    for ad_user in users_with_groups:
        email = (ad_user.get("mail") or "").strip().lower()
        ad_login = ad_user.get("ad_user")

        if not email:
            logger.info(f"Техническая учётка без email → {ad_login}")
            entry = {"__CATEGORY__": "TECH ACCOUNT", **ad_user}
            no_email.append(entry)
            continue

        if email in bk_by_email:
            logger.info(f"✔ Найден в BK: {email}")
            entry = {**ad_user, **bk_by_email[email], "__CATEGORY__": "FOUND"}
            matched.append(entry)
        else:
            logger.info(f"❌ НЕ найден в BK (уволен?) → {email}")
            entry = {"__CATEGORY__": "NOT FOUND", **ad_user}
            not_found.append(entry)

    logger.info(
        f"ИТОГО: найдено = {len(matched)}, "
        f"без email = {len(no_email)}, "
        f"не найдено = {len(not_found)}"
    )

    return matched, no_email, not_found
