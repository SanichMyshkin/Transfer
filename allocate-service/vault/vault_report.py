import os
import re
import logging
import urllib3

import requests
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("vault_kv_business_report")

VAULT_ADDR = os.getenv("VAULT_ADDR")


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

    df["code"] = df["kv"].astype(str).str.extract(r"(\d+)$", expand=False)
    df = df[df["code"].notna()].copy()
    df["code"] = df["code"].astype(str)

    df = df.sort_values("secrets", ascending=False).reset_index(drop=True)
    return df


def read_business_df(path: str) -> pd.DataFrame:
    b = pd.read_excel(path, sheet_name=0, header=None)

    b = b.iloc[:, [0, 1, 4]].copy()
    b.columns = ["service_name", "code", "business_type"]

    b["code"] = b["code"].astype(str).str.extract(r"(\d+)", expand=False)
    b = b[b["code"].notna()].copy()
    b["code"] = b["code"].astype(str)

    return b.drop_duplicates(subset=["code"], keep="first")


def main():
    if not VAULT_ADDR:
        raise SystemExit("Не задан VAULT_ADDR")

    metrics = get_vault_metrics_prometheus()
    kv_df = parse_kv_metrics_to_df(metrics)

    if kv_df.empty:
        raise SystemExit("Нет данных KV после фильтрации")

    business_df = read_business_df("business.xlsx")

    out = kv_df.merge(business_df, on="code", how="left")
    out = out[["kv", "code", "secrets", "percent", "service_name", "business_type"]]
    out["percent"] = out["percent"].round(2)

    out.to_excel("kv_usage_report.xlsx", index=False, sheet_name="KV")
    log.info(f"Excel сохранён: kv_usage_report.xlsx (строк: {len(out)})")


if __name__ == "__main__":
    main()
