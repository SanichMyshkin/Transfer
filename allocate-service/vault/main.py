import os
import re
import logging
import urllib3

import requests
import pandas as pd
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("vault_kv_sd_report")

load_dotenv()

VAULT_ADDR = os.getenv("VAULT_ADDR")
SD_FILE = os.getenv("SD_FILE", "sd.xlsx")


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
        rows.append(
            {
                "kv": kv,
                "secrets": int(m.group(2)),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    total = df["secrets"].sum()
    df["percent"] = (df["secrets"] / total) * 100 if total else 0.0

    df["code"] = df["kv"].astype(str).str.extract(r"(\d+)$", expand=False)
    df = df[df["code"].notna()].copy()
    df["code"] = df["code"].astype(str)

    return df.sort_values("secrets", ascending=False).reset_index(drop=True)


def read_sd_df(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0, header=None)
    df = df.iloc[:, [1, 2, 3]].copy()
    df.columns = ["code", "category", "sd_name"]

    df["code"] = df["code"].astype(str).str.extract(r"(\d+)", expand=False)
    df = df[df["code"].notna()].copy()
    df["code"] = df["code"].astype(str)

    df["sd_name"] = df["sd_name"].astype(str).str.strip()
    df["category"] = df["category"].astype(str).str.strip()

    return df.drop_duplicates(subset=["code"], keep="first")


def main():
    if not VAULT_ADDR:
        raise SystemExit("Не задан VAULT_ADDR")

    if not SD_FILE:
        raise SystemExit("Не задан SD_FILE")

    metrics = get_vault_metrics_prometheus()
    kv_df = parse_kv_metrics_to_df(metrics)

    if kv_df.empty:
        raise SystemExit("Нет данных KV после фильтрации")

    sd_df = read_sd_df(SD_FILE)
    out = kv_df.merge(sd_df, on="code", how="left")
    out["name"] = out["sd_name"]
    out.loc[
        out["name"].isna() | (out["name"].astype(str).str.strip() == ""), "name"
    ] = out["kv"]

    out = out.rename(
        columns={
            "name": "Наименование сервиса",
            "code": "КОД",
            "category": "Категория",
            "secrets": "Кол-во секретов",
            "percent": "% потребления",
        }
    )

    out = out[
        [
            "Наименование сервиса",
            "КОД",
            "Категория",
            "Кол-во секретов",
            "% потребления",
        ]
    ]

    out["% потребления"] = out["% потребления"].round(2)

    out.to_excel("vault_report.xlsx", index=False, sheet_name="Vault")
    log.info(f"Excel сохранён: vault_report.xlsx (строк: {len(out)})")


if __name__ == "__main__":
    main()
