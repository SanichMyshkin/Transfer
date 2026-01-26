import os
import sys
import time
import logging

import requests
import urllib3
from dotenv import load_dotenv
from prometheus_api_client import PrometheusConnect

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT_FILE = "victoriametrics_repot.xlsx"
HTTP_TIMEOUT_SEC = 60

# Безопасность: равномерная нагрузка
SLEEP_BETWEEN_QUERIES_SEC = 0.10

# Батчи: автоматически
BATCH_SIZE = 5          # сколько групп обрабатывать за один прогон
BATCH_SLEEP_SEC = 2.0   # пауза между батчами

# Скипаем жирную кучу без лейблов
SKIP_UNLABELED = True

# Живость/series (без 422): через /api/v1/series
ALIVE_LOOKBACK_SEC = 24 * 3600
SERIES_API_LIMIT = 200_000  # защита от гигантских ответов

# Интервал
INTERVAL_WINDOW = "2m"
INTERVAL_WINDOW_SEC = 120.0
FALLBACK_INTERVAL_SEC = 60.0


def safe_query(prom, q):
    log.info(f"QUERY: {q}")
    try:
        url = prom.url.rstrip("/") + "/api/v1/query"
        r = requests.get(url, params={"query": q}, verify=False, timeout=HTTP_TIMEOUT_SEC)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "success":
            log.error(f"Bad response for `{q}`: {data}")
            return None
        return data.get("data", {}).get("result", [])
    except Exception as e:
        log.error(f"Ошибка выполнения `{q}`: {e}")
        return None
    finally:
        time.sleep(SLEEP_BETWEEN_QUERIES_SEC)


def get_scalar_value(rows) -> float:
    if not rows:
        return 0.0
    try:
        return float(rows[0].get("value", [None, "0"])[1])
    except Exception:
        return 0.0


def estimate_interval_sec(avg_cnt_window: float) -> float:
    if avg_cnt_window >= 2.0:
        return INTERVAL_WINDOW_SEC / (avg_cnt_window - 1.0)
    return FALLBACK_INTERVAL_SEC


def alive_series_via_series_api(prom: PrometheusConnect, matchers: str, lookback_sec: int) -> int:
    end = int(time.time())
    start = end - lookback_sec

    url = prom.url.rstrip("/") + "/api/v1/series"
    params = {
        "match[]": "{" + matchers + "}",
        "start": str(start),
        "end": str(end),
    }

    log.info(f"SERIES_API match={params['match[]']}")
    r = requests.get(url, params=params, verify=False, timeout=HTTP_TIMEOUT_SEC)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(f"series api bad response: {data}")

    series_list = data.get("data") or []
    cnt = len(series_list)

    if SERIES_API_LIMIT and cnt > SERIES_API_LIMIT:
        log.warning(f"SERIES_API_LIMIT exceeded: got {cnt} series -> skip group")
        return 0

    return cnt


def autosize_columns(ws):
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            v = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, len(v))
        ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 60)


def write_report(rows, out_file):
    wb = Workbook()
    ws = wb.active
    ws.title = "points_per_day"

    headers = ["team", "service_id", "points_per_day_est"]
    ws.append(headers)
    bold = Font(bold=True)
    for c in ws[1]:
        c.font = bold
    ws.freeze_panes = "A2"

    for r in rows:
        ws.append(r)

    autosize_columns(ws)
    wb.save(out_file)


def build_matchers(team: str, service_id: str) -> str:
    parts = []
    parts.append('team!~".+"' if team == "" else f'team="{team}"')
    parts.append('service_id!~".+"' if service_id == "" else f'service_id="{service_id}"')
    return ", ".join(parts)


def discover_groups_single_query(prom: PrometheusConnect):
    """
    Один запрос вместо 4:
    - Берём все пары (team, service_id) как они есть (и пустые/отсутствующие тоже).
    """
    q = "count by (team, service_id) ({})"
    rows = safe_query(prom, q) or []

    groups = set()
    for r in rows:
        m = r.get("metric", {}) or {}
        team = (m.get("team") or "").strip()
        service_id = (m.get("service_id") or "").strip()
        groups.add((team, service_id))

    return sorted(groups)


def iter_batches(items, batch_size: int):
    for i in range(0, len(items), batch_size):
        yield i // batch_size + 1, items[i : i + batch_size]


def main():
    load_dotenv()
    vm_url = os.getenv("VM_URL")
    if not vm_url:
        log.error("VM_URL отсутствует")
        sys.exit(1)

    log.info(f"Подключение к VictoriaMetrics: {vm_url}")
    try:
        prom = PrometheusConnect(url=vm_url, disable_ssl=True)
        log.info("Подключение установлено.")
    except Exception as e:
        log.error(f"Не удалось подключиться к VM: {e}")
        sys.exit(1)

    log.info("Поиск групп (team/service_id) одним запросом...")
    groups = discover_groups_single_query(prom)
    log.info(f"Всего уникальных групп: {len(groups)}")

    out_rows = []
    total_batches = (len(groups) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_no, batch in iter_batches(groups, BATCH_SIZE):
        log.info(f"=== BATCH {batch_no}/{total_batches}: groups={len(batch)} ===")

        for team, service_id in batch:
            if SKIP_UNLABELED and team == "" and service_id == "":
                log.info("[GROUP] UNLABELED -> skip")
                continue

            tag = "[UNLABELED]" if team == "" and service_id == "" else f"team={team} service_id={service_id}"
            log.info(f"[GROUP] {tag}")

            matchers = build_matchers(team, service_id)

            try:
                alive_series = alive_series_via_series_api(prom, matchers, ALIVE_LOOKBACK_SEC)
            except Exception as e:
                log.error(f"SERIES_API ошибка: {e} -> skip group")
                continue

            if alive_series <= 0:
                log.info("  alive_series=0 -> пропуск")
                continue

            q_avg = f'avg(count_over_time({{{matchers}}}[{INTERVAL_WINDOW}]))'
            avg_cnt_2m = get_scalar_value(safe_query(prom, q_avg) or [])

            interval_sec = estimate_interval_sec(avg_cnt_2m)
            points_per_day = alive_series * (86400.0 / interval_sec)

            out_rows.append(
                [
                    "unlabeled" if team == "" and service_id == "" else team,
                    service_id,
                    int(points_per_day),
                ]
            )

            log.info(
                f"  alive_series_24h~{int(alive_series)} avg_cnt_2m={avg_cnt_2m:.3f} "
                f"interval~{interval_sec:.3f}s points/day~{int(points_per_day)}"
            )

        if batch_no < total_batches:
            log.info(f"Batch done -> sleep {BATCH_SLEEP_SEC}s")
            time.sleep(BATCH_SLEEP_SEC)

    if not out_rows:
        log.warning("Нет данных для отчёта.")
        sys.exit(0)

    out_rows.sort(key=lambda x: x[2], reverse=True)

    log.info(f"Сохранение файла {OUTPUT_FILE}...")
    write_report(out_rows, OUTPUT_FILE)
    log.info(f"✔ Готово. Файл {OUTPUT_FILE} создан.")


if __name__ == "__main__":
    main()
