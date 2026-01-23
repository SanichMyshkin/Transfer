import os
import re
import logging
import urllib3

import requests
import pandas as pd
from openpyxl.styles import Font
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("vault_report")

load_dotenv()
VAULT_ADDR = os.getenv("VAULT_ADDR")

SD_FILE = os.getenv("SD_FILE", "sd.xlsx")
BK_FILE = os.getenv("BK_FILE", "bk_all_users.xlsx")
OUT_FILE = os.getenv("OUT_FILE", "vault_report.xlsx")

BAN_SERVICE_IDS = [15473]          # коды, которые полностью исключаем
SKIP_EMPTY_SECRETS = True          # <<< ВАЖНО: скипать secrets == 0


def die(msg: str, code: int = 2):
    log.error(msg)
    raise SystemExit(code)


def build_ban_set(ban_list):
    if not isinstance(ban_list, (list, tuple, set)):
        die("BAN_SERVICE_IDS должен быть list / tuple / set")
    return {str(x).strip() for x in ban_list if str(x).strip()}


ban_set = build_ban_set(BAN_SERVICE_IDS)


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


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

    df["code"] = df["kv"].astype(str).str.extract(r"(\d+)$", expand=False)
    df = df[df["code"].notna()].copy()
    df["code"] = df["code"].astype(str)

    # бан-лист
    df = df[~df["code"].isin(ban_set)].copy()

    # скип пустых
    if SKIP_EMPTY_SECRETS:
        df = df[df["secrets"] > 0].copy()

    if df.empty:
        return df

    total = int(df["secrets"].sum())
    df["percent"] = (df["secrets"] / total) * 100 if total else 0.0

    return df.sort_values("secrets", ascending=False).reset_index(drop=True)


def read_sd_map(path: str) -> pd.DataFrame:
    """
    SD:
      B = code
      D = sd_name
      H = owner
      I = manager
    """
    if not path:
        die("SD_FILE не задан")
    if not os.path.exists(path):
        die(f"SD_FILE не найден: {path}")

    df = pd.read_excel(path, sheet_name=0, header=None, dtype=str).fillna("")

    out = pd.DataFrame(
        {
            "code": df.iloc[:, 1].astype(str).str.extract(r"(\d+)", expand=False),
            "sd_name": df.iloc[:, 3].map(clean_spaces),
            "owner": df.iloc[:, 7].map(clean_spaces),
            "manager": df.iloc[:, 8].map(clean_spaces),
        }
    )

    out = out[out["code"].notna()].copy()
    out["code"] = out["code"].astype(str)
    return out.drop_duplicates(subset=["code"], keep="first")


def load_bk_business_type_map(path: str) -> dict:
    """
    BK:
      A,B,C = ФИО
      AS    = тип бизнеса
    """
    if not path or not os.path.exists(path):
        log.warning("BK_FILE не найден: %s", path)
        return {}

    df = pd.read_excel(path, usecols="A:C,AS", dtype=str).fillna("")
    df.columns = ["c1", "c2", "c3", "business_type"]

    fio = (df["c2"] + " " + df["c1"] + " " + df["c3"]).map(clean_spaces)
    df["fio_key"] = fio.map(normalize_name_key)
    df["business_type"] = df["business_type"].map(clean_spaces)

    df = df[df["fio_key"] != ""].drop_duplicates("fio_key", keep="last")
    return dict(zip(df["fio_key"], df["business_type"]))


def pick_business_type(bk_map: dict, owner: str, manager: str) -> str:
    if owner:
        bt = bk_map.get(normalize_name_key(owner), "")
        if bt:
            return bt
    if manager:
        bt = bk_map.get(normalize_name_key(manager), "")
        if bt:
            return bt
    return ""


def main():
    if not VAULT_ADDR:
        die("Не задан VAULT_ADDR")

    log.info("==== START ====")
    log.info("VAULT_ADDR=%s", VAULT_ADDR)
    log.info("SKIP_EMPTY_SECRETS=%s", SKIP_EMPTY_SECRETS)
    log.info("BAN_SERVICE_IDS=%s", sorted(ban_set) if ban_set else "[]")

    metrics = get_vault_metrics_prometheus()
    kv_df = parse_kv_metrics_to_df(metrics)

    if kv_df.empty:
        die("Нет данных KV после всех фильтров")

    sd_df = read_sd_map(SD_FILE)
    bk_map = load_bk_business_type_map(BK_FILE)

    out = kv_df.merge(sd_df, on="code", how="left")

    out["service_name"] = out["sd_name"]
    out.loc[out["service_name"] == "", "service_name"] = out["kv"]

    out["owner_for_report"] = out["owner"]
    out.loc[out["owner_for_report"] == "", "owner_for_report"] = out["manager"]

    out["business_type"] = out.apply(
        lambda r: pick_business_type(
            bk_map,
            owner=str(r.get("owner") or ""),
            manager=str(r.get("manager") or ""),
        ),
        axis=1,
    )

    for _, r in out.iterrows():
        log.info(
            'AGG kv="%s" code=%s -> service="%s" owner="%s" type="%s" secrets=%d pct=%.2f',
            r["kv"],
            r["code"],
            r["service_name"],
            r["owner_for_report"] or "—",
            r["business_type"] or "—",
            int(r["secrets"]),
            float(r["percent"]),
        )

    out = out.rename(
        columns={
            "business_type": "Тип бизнеса",
            "service_name": "Наименование сервиса",
            "code": "КОД",
            "owner_for_report": "Владелец сервиса",
            "secrets": "Кол-во секретов",
            "percent": "% потребления",
        }
    )

    out = out[
        [
            "Тип бизнеса",
            "Наименование сервиса",
            "КОД",
            "Владелец сервиса",
            "Кол-во секретов",
            "% потребления",
        ]
    ]

    out["% потребления"] = out["% потребления"].round(2)

    with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="Отчет Vault")
        ws = writer.book["Отчет Vault"]
        for c in ws[1]:
            c.font = Font(bold=True)

    log.info("Итог: строк=%d, файл=%s", len(out), OUT_FILE)
    log.info("==== DONE ====")


if __name__ == "__main__":
    main()
