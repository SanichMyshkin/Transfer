import logging
import sqlite3
from config import BK_SQLITE_PATH

logger = logging.getLogger("bk_users")


def match_bk_users(ad_users):
    logger.info("=== Сопоставление BK Users с AD Users по email ===")
    logger.info(f"Открываем BK SQLite: {BK_SQLITE_PATH}")

    conn = sqlite3.connect(BK_SQLITE_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("SELECT * FROM Users").fetchall()
    conn.close()

    bk_users = [dict(r) for r in rows]
    logger.info(f"Загружено BK пользователей: {len(bk_users)}")

    bk_by_email = {(u.get("Email") or "").strip().lower(): u for u in bk_users}

    matched = []
    found_count = 0
    not_found_count = 0

    logger.info("=== Начинаем поиск совпадений AD mail ↔ BK Email ===")

    for u in ad_users:
        mail = (u.get("mail") or "").strip().lower()
        if not mail:
            logger.warning(f"AD пользователь без email: {u}")
            continue

        if mail in bk_by_email:
            bk_user = bk_by_email[mail]
            matched.append(bk_user)
            found_count += 1
            logger.info(f"✔ Найден BK пользователь: {mail} → {bk_user.get('UserName')}")
        else:
            not_found_count += 1
            logger.info(f"⚠ BK пользователь НЕ найден по email: {mail}")

    logger.info(f"=== Совпадений найдено: {found_count}, не найдено: {not_found_count}")

    return matched
