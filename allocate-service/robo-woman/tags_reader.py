import os
import logging
from collections import defaultdict

from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

load_dotenv()

ZABBIX_URL = os.getenv("ZABBIX_URL", "").rstrip("/")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN", "")

ZBX_CHUNK = 200

logger = logging.getLogger("zabbix_all_trigger_tags")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
)
logger.handlers.clear()
logger.addHandler(handler)


def die(msg: str):
    logger.error(msg)
    raise SystemExit(2)


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def fetch_active_hostids(api):
    hosts = api.host.get(output=["hostid", "status"])
    hostids = []
    for h in hosts or []:
        if int(h.get("status", 0)) != 0:
            continue
        hid = str(h.get("hostid", "")).strip()
        if hid:
            hostids.append(hid)
    return hostids


def main():
    logger.info("Старт сбора всех trigger tags (чанками по hostids)")

    if not ZABBIX_URL:
        die("ENV ZABBIX_URL пустой")
    if not ZABBIX_TOKEN:
        die("ENV ZABBIX_TOKEN пустой")

    api = ZabbixAPI(url=ZABBIX_URL)
    api.login(token=ZABBIX_TOKEN)

    try:
        hostids = fetch_active_hostids(api)
        logger.info(f"Активных хостов: {len(hostids)}")

        tag_counter = defaultdict(int)
        total_triggers_seen = 0

        for part in chunks(hostids, ZBX_CHUNK):
            trigs = api.trigger.get(
                hostids=part,
                output=["triggerid", "status"],
                selectTags="extend",
            )

            for t in trigs or []:
                if str(t.get("status", "0")) != "0":
                    continue

                total_triggers_seen += 1
                for tag in (t.get("tags") or []):
                    tag_name = (tag.get("tag") or "").strip()
                    tag_value = (tag.get("value") or "").strip()
                    if tag_name:
                        tag_counter[(tag_name, tag_value)] += 1

        logger.info(f"Триггеров просмотрено (enabled): {total_triggers_seen}")
        logger.info(f"Уникальных tag/value пар: {len(tag_counter)}")

        for (tag_name, tag_value), count in sorted(tag_counter.items()):
            logger.info(f"TAG='{tag_name}' | VALUE='{tag_value}' | TRIGGERS={count}")

    finally:
        try:
            api.logout()
        except Exception:
            pass


if __name__ == "__main__":
    main()