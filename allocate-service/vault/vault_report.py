import os
import re
import logging
import urllib3

import requests
import hvac
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("vault_kv_business_report")

VAULT_ADDR = os.getenv("VAULT_ADDR")
VAULT_TOKEN = os.getenv("VAULT_TOKEN")


def get_vault_metrics_prometheus() -> str:
    url = f"{VAULT_ADDR}/v1/sys/metrics?format=prometheus"
    r = requests.get(url, verify=False, timeout=20)
    r.raise_for_status()
    return r.text


def parse_kv_metrics_to_df(metrics_text: str) -> pd.DataFrame:
    pattern = re.compile(
        r'vault[_\s]*secret[_\s]*kv[_\s]*count\s*\{[^}]*mount_point="([^"]+)"[^}]*\}\s+(\d+)',
        re.IGNORECASE,
    )

    rows = []
    for m in pattern.finditer(metrics_text):
        kv = (m.group(1) or "").rstrip("/")
        if "test" in kv.lower():
            continue
        rows.append({"kv": kv, "secrets": int(m.group(2))})

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    total = df["secrets"].sum()
    df["percent"] = (df["secrets"] / total) * 100 if total else 0.0

    df["code"] = (
        df["kv"]
        .astype(str)
        .str.replace(r"/$", "", regex=True)
        .str.extract(r"(\d+)$", expand=False)
    )

    df = df[df["code"].notna()].copy()
    df["code"] = df["code"].astype(str)

    df = df.sort_values("secrets", ascending=False).reset_index(drop=True)
    return df


def read_business_df(path: str) -> pd.DataFrame:
    b = pd.read_excel(path, sheet_name=0, header=None)

    # A=0 (имя сервиса), B=1 (код), E=4 (тип бизнеса)
    b = b.iloc[:, [0, 1, 4]].copy()
    b.columns = ["service_name", "code", "business_type"]

    b["code"] = (
        b["code"]
        .astype(str)
        .str.strip()
        .str.extract(r"(\d+)", expand=False)
    )
    b = b[b["code"].notna()].copy()
    b["code"] = b["code"].astype(str)

    b = b.drop_duplicates(subset=["code"], keep="first")
    return b


def main():
    if not VAULT_ADDR or not VAULT_TOKEN:
        raise SystemExit("Не заданы VAULT_ADDR или VAULT_TOKEN")

    client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN, verify=False)
    if not client.is_authenticated():
        raise SystemExit("Не удалось аутентифицироваться в Vault")

    metrics = get_vault_metrics_prometheus()
    kv_df = parse_kv_metrics_to_df(metrics)

    if kv_df.empty:
        raise SystemExit("Нет данных KV после фильтров (test / парсинг метрик).")

    business_df = read_business_df("business.xlsx")

    out = kv_df.merge(business_df, on="code", how="left")

    out = out[["kv", "code", "secrets", "percent", "service_name", "business_type"]].copy()
    out["percent"] = out["percent"].round(2)

    out.to_excel("kv_usage_report.xlsx", index=False, sheet_name="KV")
    log.info(f"Excel сохранён: kv_usage_report.xlsx (строк: {len(out)})")


if __name__ == "__main__":
    main()
