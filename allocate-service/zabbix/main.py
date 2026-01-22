import os
import logging

import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

load_dotenv()

ZABBIX_URL = os.getenv("ZABBIX_URL", "").rstrip("/")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN", "")
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "zabbix_hosts.xlsx")

logger = logging.getLogger("zabbix_hosts_dump")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.handlers.clear()
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


def fetch_hosts_min(api):
    raw = api.host.get(
        output=["hostid", "host", "name", "status"],
        selectInterfaces=["ip", "dns", "main"],
    )

    rows = []
    for h in raw or []:
        iface = pick_primary_interface(h.get("interfaces") or [])
        ip = (iface.get("ip") or "").strip()
        dns = (iface.get("dns") or "").strip()

        status_raw = h.get("status", 0)
        status_int = int(status_raw) if str(status_raw).isdigit() else 0
        active = True if status_int == 0 else False  # 0=enabled, 1=disabled

        name = (h.get("name") or h.get("host") or "").strip()

        rows.append(
            {
                "name": name,
                "ip": ip,
                "dns": dns,
                "active": active,
            }
        )
    return rows


def main():
    logger.info("Подключаюсь к Zabbix...")
    api = ZabbixAPI(url=ZABBIX_URL)
    api.login(token=ZABBIX_TOKEN)
    logger.info("Ок")

    logger.info("Получаю хосты (name/ip/dns/active)...")
    rows = fetch_hosts_min(api)
    logger.info(f"Хостов: {len(rows)}")

    api.logout()
    logger.info("Сессия закрыта")

    df = pd.DataFrame(rows, columns=["name", "ip", "dns", "active"])

    enabled = int(df["active"].sum()) if not df.empty else 0
    disabled = int((~df["active"]).sum()) if not df.empty else 0
    no_ip = int((df["ip"].fillna("") == "").sum()) if not df.empty else 0
    logger.info(f"Enabled: {enabled}, Disabled: {disabled}, Без IP: {no_ip}")

    logger.info(f"Пишу Excel: {OUTPUT_XLSX}")
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="hosts")

    logger.info("Готово")
    return df


if __name__ == "__main__":
    df = main()
    print(df)  # чтобы прям увидеть в консоли
