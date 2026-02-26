import os
import logging
from collections import defaultdict

from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

load_dotenv()

ZABBIX_URL = os.getenv("ZABBIX_URL", "").rstrip("/")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN", "")

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


def main():
    logger.info("Старт сбора всех trigger tags")

    if not ZABBIX_URL:
        die("ENV ZABBIX_URL пустой")
    if not ZABBIX_TOKEN:
        die("ENV ZABBIX_TOKEN пустой")

    api = ZabbixAPI(url=ZABBIX_URL)
    api.login(token=ZABBIX_TOKEN)

    triggers = api.trigger.get(
        output=["triggerid"],
        selectTags="extend",
    )

    api.logout()

    tag_counter = defaultdict(int)

    for t in triggers or []:
        for tag in (t.get("tags") or []):
            tag_name = (tag.get("tag") or "").strip()
            tag_value = (tag.get("value") or "").strip()
            if tag_name:
                tag_counter[(tag_name, tag_value)] += 1

    logger.info(f"Всего триггеров: {len(triggers)}")
    logger.info(f"Уникальных tag/value пар: {len(tag_counter)}")

    for (tag_name, tag_value), count in sorted(tag_counter.items()):
        logger.info(f"TAG='{tag_name}' | VALUE='{tag_value}' | TRIGGERS={count}")


if __name__ == "__main__":
    main()