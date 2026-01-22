import os
import logging
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

load_dotenv()

ZABBIX_URL = os.getenv("ZABBIX_URL", "").rstrip("/")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN", "")

logger = logging.getLogger("zabbix_hosts_dump")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)

if not ZABBIX_URL or not ZABBIX_TOKEN:
    logger.error("Не найден ZABBIX_URL или ZABBIX_TOKEN в .env")
    raise SystemExit(1)


def pick_primary_interface(interfaces):
    if not interfaces:
        return {}
    for it in interfaces:
        if str(it.get("main")) == "1":
            return it
    return interfaces[0]


def normalize_macros(macros):
    out = []
    for m in macros or []:
        macro = m.get("macro")
        value = m.get("value")
        if macro:
            out.append({"macro": macro, "value": value})
    return out


def normalize_tags(tags):
    out = []
    for t in tags or []:
        tag = t.get("tag")
        val = t.get("value")
        if tag:
            out.append({"tag": tag, "value": val})
    return out


def normalize_host(h):
    iface = pick_primary_interface(h.get("interfaces") or [])

    groups = [g.get("name", "") for g in (h.get("groups") or []) if g.get("name")]
    templates = [t.get("name", "") for t in (h.get("parentTemplates") or []) if t.get("name")]

    inventory = h.get("inventory") or {}
    inventory_mode = h.get("inventory_mode")

    return {
        "hostid": str(h.get("hostid", "")),
        "host": h.get("host", ""),
        "name": h.get("name", ""),
        "status": int(h.get("status", 0)) if str(h.get("status", "")).isdigit() else h.get("status"),
        "maintenance_status": h.get("maintenance_status"),
        "proxy_hostid": h.get("proxy_hostid"),
        "description": h.get("description", ""),

        "available": h.get("available"),
        "snmp_available": h.get("snmp_available"),
        "ipmi_available": h.get("ipmi_available"),
        "jmx_available": h.get("jmx_available"),

        "ip": iface.get("ip") or "",
        "dns": iface.get("dns") or "",
        "port": iface.get("port") or "",
        "useip": iface.get("useip"),
        "interface_type": iface.get("type"),
        "interface_available": iface.get("available"),
        "interface_error": iface.get("error"),

        "groups": groups,
        "templates": templates,
        "tags": normalize_tags(h.get("tags")),

        "inventory_mode": inventory_mode,
        "inventory": inventory,

        "macros": normalize_macros(h.get("macros")),
    }


def fetch_hosts(api):
    raw = api.host.get(
        output=[
            "hostid",
            "host",
            "name",
            "status",
            "description",
            "proxy_hostid",
            "maintenance_status",
            "available",
            "snmp_available",
            "ipmi_available",
            "jmx_available",
            "inventory_mode",
        ],
        selectInterfaces=["ip", "dns", "port", "useip", "main", "type", "available", "error"],
        selectGroups=["name"],
        selectParentTemplates=["templateid", "name"],
        selectTags="extend",
        selectInventory="extend",
        selectMacros=["macro", "value"],
    )
    return [normalize_host(h) for h in (raw or [])]


def main():
    logger.info("Подключаюсь к Zabbix...")
    api = ZabbixAPI(url=ZABBIX_URL)
    api.login(token=ZABBIX_TOKEN)
    logger.info("Ок")

    logger.info("Получаю хосты + данные...")
    hosts = fetch_hosts(api)
    logger.info(f"Хостов: {len(hosts)}")

    enabled = sum(1 for h in hosts if h.get("status") == 0)
    disabled = sum(1 for h in hosts if h.get("status") == 1)
    no_ip = sum(1 for h in hosts if not h.get("ip"))
    inv_filled = sum(1 for h in hosts if (h.get("inventory") or {}).get("os") or (h.get("inventory") or {}).get("name"))
    logger.info(f"Enabled: {enabled}, Disabled: {disabled}, Без IP: {no_ip}, Inventory хоть что-то: {inv_filled}")

    api.logout()
    logger.info("Сессия закрыта")

    return hosts


if __name__ == "__main__":
    hosts = main()
