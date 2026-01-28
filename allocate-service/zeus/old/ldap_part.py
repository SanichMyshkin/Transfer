# ldap_part.py
import os
import logging
from ldap3 import Server, Connection, SUBTREE
from ldap3.utils.conv import escape_filter_chars
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)
AD_SERVER = os.getenv("AD_SERVER")
AD_USER = os.getenv("AD_USER")
AD_PASSWORD = os.getenv("AD_PASSWORD")
AD_GROUP_SEARCH_BASE = os.getenv("AD_GROUP_SEARCH_BASE")


def ldap_connect():
    logger.info("=== LDAP CONNECT ===")
    logger.info(f"SERVER={AD_SERVER}")
    logger.info(f"USER={AD_USER}")
    logger.info(f"BASE={AD_GROUP_SEARCH_BASE}")
    server = Server(AD_SERVER, port=389, get_info=None)
    conn = Connection(
        server, AD_USER, AD_PASSWORD, auto_bind=True, auto_referrals=False
    )
    logger.info("LDAP CONNECTED")
    return conn


def _clean(prefix: str) -> str:
    return prefix.rstrip("*") if prefix else ""


def _build_filter(prefix: str) -> str:
    core = _clean(prefix)
    esc = escape_filter_chars(core)
    pattern = f"*{esc}*"
    parts = [
        f"(cn={pattern})",
        f"(displayName={pattern})",
        f"(sn={pattern})",
        f"(uid={pattern})",
    ]
    flt = "(|" + "".join(parts) + ")"
    logger.info(f"FILTER CORE={core}")
    logger.info(f"FILTER={flt}")
    return flt


def _extract_cn_from_dn(dn: str) -> str:
    try:
        first = dn.split(",")[0]
        return first.split("=", 1)[1]
    except Exception:
        return ""


def find_groups(conn, prefix: str):
    logger.info(f"=== FIND GROUPS prefix='{prefix}' ===")
    if not prefix:
        logger.warning("PREFIX EMPTY")
        return []
    flt = _build_filter(prefix)
    logger.info(f"SEARCH_BASE={AD_GROUP_SEARCH_BASE}")
    logger.info(f"SEARCH_FILTER={flt}")
    entry_generator = conn.extend.standard.paged_search(
        search_base=AD_GROUP_SEARCH_BASE,
        search_filter=flt,
        search_scope=SUBTREE,
        attributes=["cn", "distinguishedName", "member"],
        get_operational_attributes=True,
        paged_size=1000,
        generator=True,
    )
    results = []
    for entry in entry_generator:
        if entry.get("type") != "searchResEntry":
            continue
        attrs = entry.get("attributes", {})
        cn_val = attrs.get("cn", "")
        cn = cn_val[0] if isinstance(cn_val, list) else cn_val
        dn = entry.get("dn", "")
        members = attrs.get("member", [])
        logger.info(f"[GROUP] CN={cn} DN={dn} MEMBERS={len(members)}")
        results.append(
            {
                "cn": cn,
                "dn": dn,
                "members": members,
            }
        )
    logger.info(f"TOTAL GROUPS FOUND={len(results)}")
    return results


def _fetch_user(conn, dn: str):
    logger.info(f"FETCH USER DN={dn}")

    conn.search(
        search_base=dn,
        search_filter="(objectClass=user)",
        search_scope=SUBTREE,
        attributes=["sAMAccountName", "displayName", "mail"],
    )

    if not conn.entries:
        logger.info("NO USER ENTRY")
        return None

    attrs = conn.entries[0].entry_attributes_as_dict

    return {
        "user": attrs.get("sAMAccountName", [""])[0],
        "displayName": attrs.get("displayName", [""])[0],
        "mail": attrs.get("mail", [""])[0].lower() if attrs.get("mail") else "",
        "cn": dn,
    }


def get_project_ad_users(conn, prefix: str):
    logger.info(f"=== PROJECT USERS prefix='{prefix}' ===")
    groups = find_groups(conn, prefix)
    all_users = []
    for g in groups:
        logger.info(f"PROCESS GROUP {g['cn']}")
        for mdn in g["members"]:
            user = _fetch_user(conn, mdn)
            if user:
                user["group"] = g["cn"]
                all_users.append(user)
    uniq = {u["mail"]: u for u in all_users if u["mail"]}
    logger.info(f"UNIQUE USERS COUNT={len(uniq)}")
    return list(uniq.values())
