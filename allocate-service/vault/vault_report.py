import os
import re
import logging
from dotenv import load_dotenv

import hvac
import requests
import xlsxwriter

load_dotenv()

VAULT_ADDR = os.getenv("VAULT_ADDR")
VAULT_TOKEN = os.getenv("VAULT_TOKEN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("vault_kv_report")


def get_vault_metrics_prometheus() -> str | None:
    """
    Получаем метрики Vault в формате Prometheus.
    Обычно работает /v1/sys/metrics?format=prometheus.
    """
    url = f"{VAULT_ADDR}/v1/sys/metrics?format=prometheus"
    try:
        r = requests.get(url, verify=False, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        log.error(f"Ошибка получения метрик: {e}")
        return None


def parse_kv_metrics(metrics_text: str):
    """
    Ищем метрику вида:
      vault_secret_kv_count{...,mount_point="kv/",...} 123

    Разные версии могут писать через '_'/' ' — делаем regex гибким.
    """
    pattern = re.compile(
        r'vault[_\s]*secret[_\s]*kv[_\s]*count\s*\{[^}]*mount_point="([^"]+)"[^}]*\}\s+(\d+)',
        re.IGNORECASE,
    )

    rows = []
    total = 0
    for m in pattern.finditer(metrics_text):
        mount = (m.group(1) or "").rstrip("/")  # kv/ -> kv
        count = int(m.group(2))
        rows.append({"kv": mount, "count": count})
        total += count

    # Проценты
    for r in rows:
        r["percent"] = (r["count"] / total * 100.0) if total else 0.0

    # Сортировка по убыванию кол-ва секретов
    rows.sort(key=lambda x: x["count"], reverse=True)

    return rows, total


def write_excel_one_sheet(filename: str, rows, total: int):
    wb = xlsxwriter.Workbook(filename)
    ws = wb.add_worksheet("KV")

    # Форматы
    fmt_header = wb.add_format({"bold": True})
    fmt_int = wb.add_format({"num_format": "0"})
    fmt_pct = wb.add_format({"num_format": "0.00%"})
    fmt_bold = wb.add_format({"bold": True})

    # Колонки (одна “страница” / один лист)
    ws.write(0, 0, "KV mount", fmt_header)
    ws.write(0, 1, "Secrets", fmt_header)
    ws.write(0, 2, "% of total", fmt_header)

    # Данные
    for i, r in enumerate(rows, start=1):
        ws.write(i, 0, r["kv"])
        ws.write_number(i, 1, r["count"], fmt_int)
        ws.write_number(i, 2, r["percent"] / 100.0, fmt_pct)  # xlsx проценты = доля

    # Итого
    last_row = len(rows) + 2
    ws.write(last_row, 0, "TOTAL", fmt_bold)
    ws.write_number(last_row, 1, total, fmt_bold)
    ws.write_number(last_row, 2, 1.0 if total else 0.0, fmt_bold)  # 100%

    # Авто-ширина + “влезть на 1 страницу при печати”
    ws.set_column(0, 0, 35)
    ws.set_column(1, 1, 12)
    ws.set_column(2, 2, 12)
    ws.freeze_panes(1, 0)
    ws.set_landscape()
    ws.fit_to_pages(1, 1)  # 1 страница по ширине и высоте

    wb.close()
    log.info(f"Excel сохранён: {filename}")


def main():
    if not VAULT_ADDR or not VAULT_TOKEN:
        raise SystemExit("Не заданы VAULT_ADDR или VAULT_TOKEN")

    client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN, verify=CA_CERT)
    if not client.is_authenticated():
        raise SystemExit("Не удалось аутентифицироваться в Vault")

    metrics = get_vault_metrics_prometheus()
    if not metrics:
        raise SystemExit("Не удалось получить метрики Vault")

    rows, total = parse_kv_metrics(metrics)
    log.info(f"KV-монтов: {len(rows)}, секретов всего: {total}")

    write_excel_one_sheet("vault_report.xlsx", rows, total)


if __name__ == "__main__":
    main()
