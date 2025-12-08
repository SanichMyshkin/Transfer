import ssl
import time
import logging

from ldap3 import Server, Connection, ALL, SUBTREE, Tls
from ldap3.utils.conv import escape_filter_chars

from config import AD_SERVER, AD_USER, AD_PASSWORD, AD_BASE, CA_CERT

log = logging.getLogger("nexus_ldap")


def safe_get(attr_dict, key):
    val = attr_dict.get(key, "")
    if isinstance(val, (list, tuple)):
        return val[0] if val else ""
    return val or ""


def connect_ldap():
    if not AD_SERVER or not AD_USER or not AD_PASSWORD or not AD_BASE:
        raise RuntimeError(
            "Не заданы параметры LDAP (AD_SERVER/AD_USER/AD_PASSWORD/AD_BASE) в .env"
        )

    log.info(f"Подключаемся к LDAP: {AD_SERVER}")
    tls = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=CA_CERT)
    server = Server(AD_SERVER, use_ssl=True, get_info=ALL, tls=tls)
    conn = Connection(server, AD_USER, AD_PASSWORD, auto_bind=True)
    log.info("Подключение к LDAP установлено")
    return conn


def get_users_from_ad_group(conn, group_name: str):
    start_time = time.perf_counter()
    name_esc = escape_filter_chars(group_name)

    group_filter = (
        f"(&(objectClass=group)"
        f"(|(cn={name_esc})(sAMAccountName={name_esc})(name={name_esc})))"
    )

    log.info(f"Поиск группы в LDAP: {group_name}")
    conn.search(
        search_base=AD_BASE,
        search_filter=group_filter,
        search_scope=SUBTREE,
        attributes=["distinguishedName", "cn", "member"],
    )

    if not conn.entries:
        log.warning(f"Группа '{group_name}' не найдена в AD")
        return {"group": group_name, "found": False, "members": []}

    entry = conn.entries[0]
    members = entry.member.values if "member" in entry else []
    users = []

    if not members:
        log.info(f"Группа '{group_name}' пуста")
        return {"group": group_name, "found": True, "members": []}

    for dn in members:
        try:
            conn.search(
                search_base=dn,
                search_filter="(objectClass=user)",
                search_scope=SUBTREE,
                attributes=["sAMAccountName", "displayName", "mail"],
            )
            if conn.entries:
                u = conn.entries[0]
                a = u.entry_attributes_as_dict
                users.append(
                    {
                        "ad_group": group_name,
                        "user": safe_get(a, "sAMAccountName"),
                        "displayName": safe_get(a, "displayName"),
                        "mail": safe_get(a, "mail").lower(),
                        "user_dn": dn,
                    }
                )
        except Exception as e:
            log.error(f"Ошибка при обработке DN {dn} в группе '{group_name}': {e}")

    elapsed = time.perf_counter() - start_time
    log.info(
        f"Группа '{group_name}' обработана: {len(users)} пользователей, {elapsed:.2f} сек."
    )
    return {"group": group_name, "found": True, "members": users}


# ================================


def fetch_ldap_group_members(ad_groups: list[str]):
    unique_groups = sorted({g for g in ad_groups if g})

    if not unique_groups:
        log.info("Список AD-групп пуст — LDAP запросы не выполняются")
        return []

    conn = connect_ldap()
    all_members = []

    total = len(unique_groups)
    log.info(f"=== Обработка {total} AD-групп через LDAP ===")

    try:
        for idx, group in enumerate(unique_groups, start=1):
            log.info(f"[{idx}/{total}] Обрабатывается AD-группа: {group}")
            try:
                g_data = get_users_from_ad_group(conn, group)
                if g_data["found"]:
                    all_members.extend(g_data["members"])
            except Exception as e:
                log.error(f"Ошибка при запросе группы '{group}': {e}")
    finally:
        conn.unbind()
        log.info("LDAP-соединение закрыто")

    log.info(f"Всего пользователей (все группы, неуникальные): {len(all_members)}")
    return all_members


def aggregate_users_by_groups(ad_group_members: list[dict]):
    index = {}

    for m in ad_group_members:
        user = (m.get("user") or "").strip()
        mail = (m.get("mail") or "").strip().lower()
        dn = (m.get("user_dn") or "").strip()
        ad_group = m.get("ad_group", "").strip()

        if not (user or mail or dn):
            continue

        if user:
            key = f"user:{user.lower()}"
        elif mail:
            key = f"mail:{mail}"
        else:
            key = f"dn:{dn.lower()}"

        if key not in index:
            index[key] = {
                "user": user,
                "displayName": m.get("displayName", ""),
                "mail": mail,
                "ad_groups": set(),
            }

        if ad_group:
            index[key]["ad_groups"].add(ad_group)

    result = []
    for data in index.values():
        result.append(
            {
                "user": data["user"],
                "displayName": data["displayName"],
                "mail": data["mail"],
                "ad_groups": ", ".join(sorted(data["ad_groups"])),
            }
        )

    log.info(f"Уникальных пользователей после агрегации: {len(result)}")
    return result
