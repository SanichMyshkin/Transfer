import os
import re
import logging
import urllib3
from dotenv import load_dotenv

import hvac
import requests
import xlsxwriter

load_dotenv()

VAULT_ADDR = os.getenv("VAULT_ADDR")
VAULT_TOKEN = os.getenv("VAULT_TOKEN")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("vault_kv_report")


def get_vault_metrics_prometheus():
    url = f"{VAULT_ADDR}/v1/sys/metrics?format=prometheus"
    try:
        r = requests.get(url, verify=False, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        log.error(f"Ошибка получения метрик: {e}")
        return None


def make_code(mount: str) -> str:
    m = (mount or "").rstrip("/")
    if m.startswith("kv-"):
        m = m[3:]
    return m


def parse_kv_metrics(metrics_text: str):
    pattern = re.compile(
        r'vault[_\s]*secret[_\s]*kv[_\s]*count\s*\{[^}]*mount_point="([^"]+)"[^}]*\}\s+(\d+)',
        re.IGNORECASE,
    )

    rows = []
    total = 0

    for m in pattern.finditer(metrics_text):
        mount = (m.group(1) or "").rstrip("/")
        if "test" in mount.lower():
            continue
        count = int(m.group(2))
        rows.append({"kv": mount, "code": make_code(mount), "count": count})
        total += count

    for r in rows:
        r["percent"] = (r["count"] / total * 100.0) if total else 0.0

    rows.sort(key=lambda x: x["count"], reverse=True)
    return rows, total


def write_excel_one_sheet(filename: str, rows, total: int):
    wb = xlsxwriter.Workbook(filename)
    ws = wb.add_worksheet("KV")

    fmt_header = wb.add_format({"bold": True})
    fmt_int = wb.add_format({"num_format": "0"})
    fmt_pct = wb.add_format({"num_format": "0.00%"})
    fmt_bold = wb.add_format({"bold": True})

    ws.write(0, 0, "kv", fmt_header)
    ws.write(0, 1, "code", fmt_header)
    ws.write(0, 2, "secrets", fmt_header)
    ws.write(0, 3, "%", fmt_header)

    for i, r in enumerate(rows, start=1):
        ws.write(i, 0, r["kv"])
        ws.write(i, 1, r["code"])
        ws.write_number(i, 2, r["count"], fmt_int)
        ws.write_number(i, 3, r["percent"] / 100.0, fmt_pct)

    last_row = len(rows) + 2
    ws.write(last_row, 0, "TOTAL", fmt_bold)
    ws.write(last_row, 2, total, fmt_bold)
    ws.write_number(last_row, 3, 1.0 if total else 0.0, fmt_bold)

    ws.set_column(0, 0, 35)
    ws.set_column(1, 1, 25)
    ws.set_column(2, 2, 12)
    ws.set_column(3, 3, 10)

    ws.freeze_panes(1, 0)
    ws.set_landscape()
    ws.fit_to_pages(1, 1)

    wb.close()
    log.info(f"Excel сохранён: {filename}")


def main():
    if not VAULT_ADDR or not VAULT_TOKEN:
        raise SystemExit("Не заданы VAULT_ADDR или VAULT_TOKEN")

    client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN, verify=False)
    if not client.is_authenticated():
        raise SystemExit("Не удалось аутентифицироваться в Vault")

    metrics = get_vault_metrics_prometheus()
    if not metrics:
        raise SystemExit("Не удалось получить метрики Vault")

    rows, total = parse_kv_metrics(metrics)
    log.info(f"KV-монтов (без test): {len(rows)}, секретов всего (без test): {total}")

    write_excel_one_sheet("kv_usage_report.xlsx", rows, total)


if __name__ == "__main__":
    main()
