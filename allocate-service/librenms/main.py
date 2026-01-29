import os
import logging
import requests
from urllib.parse import quote
from dotenv import load_dotenv

load_dotenv()

LIBRENMS_URL = os.getenv("LIBRENMS_URL", "").rstrip("/")
LIBRENMS_TOKEN = os.getenv("LIBRENMS_TOKEN", "").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("librenms")


session = requests.Session()
session.headers.update({
    "X-Auth-Token": LIBRENMS_TOKEN,
    "Accept": "application/json",
})

log.info("Запрос списка групп устройств")

resp = session.get(
    f"{LIBRENMS_URL}/api/v0/devicegroups",
    timeout=30,
    verify=False,
)
resp.raise_for_status()
data = resp.json()

groups = data.get("groups", [])
log.info("Найдено групп: %d", len(groups))

total_devices = 0

for group in groups:
    name = group.get("name", "").strip()
    if not name:
        continue

    log.info("Группа: %s", name)

    resp = session.get(
        f"{LIBRENMS_URL}/api/v0/devicegroups/{quote(name, safe='')}",
        params={"full": 1},
        timeout=30,
        verify=False,
    )
    resp.raise_for_status()
    data = resp.json()

    devices = data.get("devices", [])
    total_devices += len(devices)

    log.info("Устройств в группе: %d", len(devices))

    for d in devices[:5]:
        hostname = d.get("hostname") or d.get("sysName") or d.get("device_id")
        log.info("  device: %s", hostname)

log.info("Итого устройств (суммарно по группам): %d", total_devices)
