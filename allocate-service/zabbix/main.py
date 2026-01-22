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


def normalize_host(h):
    hostid = str(h.get("hostid", ""))
    host = h.get("host", "")
    name = h.get("name", "")
    status = int(h.get("status", 0)) if str(h.get("status", "")).isdigit() else h.get("status")

    iface = pick_primary_interface(h.get("interfaces") or [])
    ip = iface.get("ip") or ""
    dns = iface.get("dns") or ""
    port = iface.get("port") or ""
    useip = iface.get("useip")

    groups = [g.get("name", "") for g in (h.get("groups") or []) if g.get("name")]
    templates = [t.get("name", "") for t in (h.get("parentTemplates") or []) if t.get("name")]
    tags = []
    for t in h.get("tags") or []:
        tag = t.get("tag")
        val = t.get("value")
        if tag:
            tags.append({"tag": tag, "value": val})

    return {
        "hostid": hostid,
        "host": host,
        "name": name,
        "status": status,
        "ip": ip,
        "dns": dns,
        "port": port,
        "useip": useip,
        "groups": groups,
        "templates": templates,
        "tags": tags,
    }


def fetch_hosts(api):
    raw = api.host.get(
        output=["hostid", "host", "name", "status"],
        selectInterfaces=["ip", "dns", "port", "useip", "main"],
        selectGroups=["name"],
        selectParentTemplates=["templateid", "name"],
        selectTags="extend",
    )
    return [normalize_host(h) for h in raw or []]


def main():
    logger.info("Подключаюсь к Zabbix...")
    api = ZabbixAPI(url=ZABBIX_URL)
    api.login(token=ZABBIX_TOKEN)
    logger.info("Ок")

    logger.info("Получаю хосты...")
    hosts = fetch_hosts(api)
    logger.info(f"Хостов: {len(hosts)}")

    enabled = sum(1 for h in hosts if h.get("status") == 0)
    disabled = sum(1 for h in hosts if h.get("status") == 1)
    no_ip = sum(1 for h in hosts if not h.get("ip"))
    logger.info(f"Enabled: {enabled}, Disabled: {disabled}, Без IP: {no_ip}")

    api.logout()
    logger.info("Сессия закрыта")

    return hosts


if __name__ == "__main__":
    hosts = main()
