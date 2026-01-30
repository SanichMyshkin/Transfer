import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone

import requests
import urllib3
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT_FILE = os.getenv("OUTPUT_FILE", "victoriametrics_samples_24h.xlsx")
HTTP_TIMEOUT_SEC = 30
SLEEP_BETWEEN_QUERIES_SEC = float(os.getenv("SLEEP_BETWEEN_QUERIES_SEC", "0.2"))
MAX_METRICS = int(os.getenv("MAX_METRICS", "0"))


def http_get_json(base_url: str, path: str, params: dict | None = None):
    url = base_url.rstrip("/") + path
    try:
        r = requests.get(url, params=params or {}, verify=False, timeout=HTTP_TIMEOUT_SEC)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "success":
            log.error(f"Bad response: {path} params={params} body={data}")
            return None
        return data
    except Exception as e:
        log.error(f"HTTP error: {path} params={params} err={e}")
        return None


def get_metric_names(vm_url: str) -> list[str]:
    data = http_get_json(vm_url, "/api/v1/label/__name__/values")
    if not data:
        return []
    values = data.get("data", [])
    out = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out.append(s)
    out = sorted(set(out))
    return out


def query_range(vm_url: str, query: str, start_ts: float, end_ts: float, step_sec: int):
    params = {
        "query": query,
        "start": str(start_ts),
        "end": str(end_ts),
        "step": str(step_sec),
    }
    data = http_get_json(vm_url, "/api/v1/query_range", params=params)
    if not data:
        return None
    return data.get("data", {}).get("result", [])


def compute_samples_24h_for_metric(vm_url: str, metric_name: str, end_dt: datetime, hours: int = 24) -> tuple[float, int]:
    start_dt = end_dt - timedelta(hours=hours)
    start_ts = start_dt.timestamp()
    end_ts = end_dt.timestamp()
    q = f'sum(count_over_time({{__name__="{metric_name}"}}[1h]))'

    rows = query_range(vm_url, q, start_ts, end_ts, 3600) or []
    total = 0.0
    points = 0

    for r in rows:
        values = r.get("values") or []
        for tv in values:
            if not isinstance(tv, list) or len(tv) < 2:
                continue
            v = tv[1]
            try:
                total += float(v)
                points += 1
            except Exception:
                continue

    return total, points


def write_report(rows: list[tuple[str, float, int]], out_file: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "samples_24h"

    headers = ["metric", "samples_24h", "hours_points"]
    bold = Font(bold=True)

    ws.append(headers)
    for c in ws[1]:
        c.font = bold

    for name, samples, points in rows:
        ws.append([name, int(round(samples)), points])

    wb.save(out_file)


def main():
    load_dotenv()
    vm_url = os.getenv("VM_URL", "").strip()
    if not vm_url:
        log.error("VM_URL отсутствует")
        sys.exit(1)

    log.info(f"VM: {vm_url}")
    log.info("Получаю список метрик...")
    names = get_metric_names(vm_url)
    if not names:
        log.error("Не удалось получить список метрик или список пуст")
        sys.exit(1)

    if MAX_METRICS > 0:
        names = names[:MAX_METRICS]

    log.info(f"Метрик найдено: {len(names)}")

    end_dt = datetime.now(timezone.utc)

    out_rows = []
    for i, name in enumerate(names, 1):
        log.info(f"[{i}/{len(names)}] metric={name}")
        samples, points = compute_samples_24h_for_metric(vm_url, name, end_dt=end_dt, hours=24)
        out_rows.append((name, samples, points))
        if SLEEP_BETWEEN_QUERIES_SEC > 0:
            time.sleep(SLEEP_BETWEEN_QUERIES_SEC)

    log.info(f"Сохранение файла {OUTPUT_FILE}...")
    write_report(out_rows, OUTPUT_FILE)
    log.info(f"✔ Готово. Файл {OUTPUT_FILE} создан.")


if __name__ == "__main__":
    main()
