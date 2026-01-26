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
SLEEP_BETWEEN_QUERIES_SEC = float(os.getenv("SLEEP_BETWEEN_QUERIES_SEC", "0.08"))

# Крохотный прогон
MAX_GROUPS = int(os.getenv("MAX_GROUPS", "10"))          # 0 = все
START_FROM = int(os.getenv("START_FROM", "0"))
STOP_AFTER_SECONDS = int(os.getenv("STOP_AFTER_SECONDS", "60"))  # 0 = без

# Скип жирной кучи без лейблов
SKIP_UNLABELED = os.getenv("SKIP_UNLABELED", "true").strip().lower() in {"1", "true", "yes", "y", "on"}

# Живость: через /api/v1/series (индекс, не читает миллиарды сэмплов)
ALIVE_LOOKBACK_SEC = int(os.getenv("ALIVE_LOOKBACK_SEC", str(24 * 3600)))  # 24h
SERIES_API_LIMIT = int(os.getenv("SERIES_API_LIMIT", "200000"))            # защита от гигантских ответов

# Интервал
INTERVAL_WINDOW = "2m"
INTERVAL_WINDOW_SEC = 120.0
FALLBACK_INTERVAL_SEC = float(os.getenv("FALLBACK_INTERVAL_SEC", "60"))


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
        if SLEEP_BETWEEN_QUERIES_SEC > 0:
            time.sleep(SLEEP_BETWEEN_QUERIES_SEC)


def label(metric: dict, key: str) -> str:
    v = metric.get(key)
    return "" if v is None or str(v).strip() == "" else str(v).strip()


def discover_groups(prom):
    groups = set()

    # ВАЖНО: тут у тебя был дубль — исправлено
    queries = [
        'count by (team, service_id) ({team=~".+", service_id=~".+"})',
        'count by (team, service_id) ({team!~".+", service_id=~".+"})',
        'count by (team, service_id) ({team=~".+", service_id!~".+"})',
        'count by (team, service_id) ({team!~".+", service_id!~".+"})',
    ]

    for i, q in enumerate(queries, start=1):
        log.info(f"DISCOVER[{i}/4]")
        rows = safe_query(prom, q) or []
        for r in rows:
            m = r.get("metric", {}) or {}
            groups.add((label(m, "team"), label(m, "service_id")))

    groups = sorted(groups)
    if START_FROM > 0:
        groups = groups[START_FROM:]
    if MAX_GROUPS and len(groups) > MAX_GROUPS:
        log.warning(f"MAX_GROUPS={MAX_GROUPS}: урезаем группы {len(groups)} -> {MAX_GROUPS}")
        groups = groups[:MAX_GROUPS]
    return groups


def build_matchers(team: str, service_id: str) -> str:
    return ", ".join(
        [
            'team!~".+"' if team == "" else f'team="{team}"',
            'service_id!~".+"' if service_id == "" else f'service_id="{service_id}"',
        ]
    )


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
    """
    Считает кол-во series, у которых были точки в окне [now-lookback, now],
    через /api/v1/series (индекс). Это как раз замена count(count_over_time(..[24h])),
    чтобы НЕ ловить 422 maxSamplesPerQuery.
    """
    end = int(time.time())
    start = end - lookback_sec

    url = prom.url.rstrip("/") + "/api/v1/series"
    params = {
        "match[]": "{" + matchers + "}",
        "start": str(start),
        "end": str(end),
    }

    log.info(f"SERIES_API: {url} match={params['match[]']} start={start} end={end}")

    r = requests.get(url, params=params, verify=False, timeout=HTTP_TIMEOUT_SEC)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(f"series api bad response: {data}")

    series_list = data.get("data") or []
    cnt = len(series_list)

    # Защита: если группа супер-жирная, не тащим всё дальше
    if SERIES_API_LIMIT and cnt > SERIES_API_LIMIT:
        log.warning(f"SERIES_API_LIMIT={SERIES_API_LIMIT} exceeded: got {cnt} series -> скипаем группу")
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

    log.info(
        f"MAX_GROUPS={MAX_GROUPS} START_FROM={START_FROM} STOP_AFTER_SECONDS={STOP_AFTER_SECONDS} "
        f"SKIP_UNLABELED={SKIP_UNLABELED} ALIVE_LOOKBACK_SEC={ALIVE_LOOKBACK_SEC}"
    )

    groups = discover_groups(prom)
    log.info(f"Групп к обработке: {len(groups)}")

    started = time.time()
    out_rows = []

    for idx, (team, service_id) in enumerate(groups, start=1):
        if STOP_AFTER_SECONDS and (time.time() - started) > STOP_AFTER_SECONDS:
            log.warning("STOP_AFTER_SECONDS сработал -> останавливаемся")
            break

        if SKIP_UNLABELED and team == "" and service_id == "":
            log.info("[GROUP] UNLABELED -> skip")
            continue

        tag = "[UNLABELED]" if team == "" and service_id == "" else f"team={team} service_id={service_id}"
        log.info(f"[{idx}/{len(groups)}] [GROUP] {tag}")

        m = build_matchers(team, service_id)

        # 1) alive series за 24h (без 422)
        try:
            alive_series = alive_series_via_series_api(prom, m, ALIVE_LOOKBACK_SEC)
        except Exception as e:
            log.error(f"SERIES_API ошибка: {e} -> пропуск группы")
            continue

        if alive_series <= 0:
            log.info("  alive_series=0 -> пропуск")
            continue

        # 2) интервал по 2m (лёгкий запрос)
        q_avg = f'avg(count_over_time({{{m}}}[{INTERVAL_WINDOW}]))'
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

    if not out_rows:
        log.warning("Нет данных для отчёта.")
        sys.exit(0)

    out_rows.sort(key=lambda x: x[2], reverse=True)

    log.info(f"Сохранение файла {OUTPUT_FILE}...")
    write_report(out_rows, OUTPUT_FILE)
    log.info(f"✔ Готово. Файл {OUTPUT_FILE} создан.")


if __name__ == "__main__":
    main()
