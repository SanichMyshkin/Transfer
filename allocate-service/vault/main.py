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
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("vault_report")

load_dotenv()
VAULT_ADDR = os.getenv("VAULT_ADDR")

SD_FILE = os.getenv("SD_FILE", "sd.xlsx")
BK_FILE = os.getenv("BK_FILE", "bk_all_users.xlsx")
OUT_FILE = os.getenv("OUT_FILE", "vault_report.xlsx")

BAN_SERVICE_IDS = [15473]
BAN_BUSINESS_TYPES = []

SKIP_EMPTY_SECRETS = True
SKIP_EMPTY_BUSINESS_TYPE = True


def die(msg: str, code: int = 2):
    log.error(msg)
    raise SystemExit(code)


def build_ban_set(ban_list):
    if not isinstance(ban_list, (list, tuple, set)):
        die("BAN_SERVICE_IDS должен быть list / tuple / set")
    return {str(x).strip() for x in ban_list if str(x).strip()}


ban_set = build_ban_set(BAN_SERVICE_IDS)
ban_business_set = {
    " ".join(str(x).replace(",", " ").split())
    for x in BAN_BUSINESS_TYPES
    if " ".join(str(x).replace(",", " ").split())
}


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


def parse_kv_metrics(metrics_text: str) -> pd.DataFrame:
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

    return pd.DataFrame(rows)


def read_sd_map(path: str) -> pd.DataFrame:
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
        }
    )

    out = out[out["code"].notna()].copy()
    out["code"] = out["code"].astype(str)
    return out.drop_duplicates(subset=["code"], keep="first")


def load_bk_business_type_map(path: str) -> dict:
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


def main():
    if not VAULT_ADDR:
        die("Не задан VAULT_ADDR")

    log.info("==== START ====")
    log.info("VAULT_ADDR=%s", VAULT_ADDR)
    log.info("SKIP_EMPTY_SECRETS=%s", SKIP_EMPTY_SECRETS)
    log.info("SKIP_EMPTY_BUSINESS_TYPE=%s", SKIP_EMPTY_BUSINESS_TYPE)
    log.info("BAN_SERVICE_IDS=%s", sorted(ban_set) if ban_set else "[]")

    metrics = get_vault_metrics_prometheus()
    base_df = parse_kv_metrics(metrics)

    if base_df.empty:
        die("Нет данных KV в метриках (после пропуска test)")

    unacc_rows = []

    base_df["code"] = base_df["kv"].astype(str).str.extract(r"(\d+)$", expand=False)
    no_code = base_df[base_df["code"].isna()].copy()
    if not no_code.empty:
        no_code["reason"] = "no_code_in_kv"
        no_code["detail"] = "cannot extract code via r'(\\d+)$' from kv"
        unacc_rows.append(no_code)

    df = base_df[base_df["code"].notna()].copy()
    df["code"] = df["code"].astype(str)

    banned = df[df["code"].isin(ban_set)].copy()
    if not banned.empty:
        banned["reason"] = "banned_service_id"
        banned["detail"] = "code in BAN_SERVICE_IDS"
        unacc_rows.append(banned)

    df = df[~df["code"].isin(ban_set)].copy()

    if SKIP_EMPTY_SECRETS:
        empty_sec = df[df["secrets"] <= 0].copy()
        if not empty_sec.empty:
            empty_sec["reason"] = "empty_secrets"
            empty_sec["detail"] = "SKIP_EMPTY_SECRETS=True and secrets<=0"
            unacc_rows.append(empty_sec)
        df = df[df["secrets"] > 0].copy()

    if df.empty:
        unaccounted = (
            pd.concat(unacc_rows, ignore_index=True) if unacc_rows else pd.DataFrame()
        )
        if not unaccounted.empty:
            unaccounted = unaccounted.rename(
                columns={"kv": "kv", "code": "КОД", "secrets": "Кол-во секретов"}
            )
            unaccounted = unaccounted[
                ["kv", "КОД", "Кол-во секретов", "reason", "detail"]
            ]
        with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as writer:
            pd.DataFrame([{"msg": "Нет данных после фильтров"}]).to_excel(
                writer, index=False, sheet_name="Отчет Vault"
            )
            if not unaccounted.empty:
                unaccounted.to_excel(writer, index=False, sheet_name="Unaccounted")
                ws_u = writer.book["Unaccounted"]
                for c in ws_u[1]:
                    c.font = Font(bold=True)
        die("Нет данных KV после всех фильтров (учтённых)")

    sd_df = read_sd_map(SD_FILE)
    bk_map = load_bk_business_type_map(BK_FILE)

    out = df.merge(sd_df, on="code", how="left")

    out["service_name"] = out["sd_name"]
    out.loc[out["service_name"] == "", "service_name"] = out["kv"]

    out["owner_for_report"] = out["owner"]

    out["business_type"] = out.apply(
        lambda r: (
            bk_map.get(normalize_name_key(str(r.get("owner") or "")), "")
            if str(r.get("owner") or "")
            else ""
        ),
        axis=1,
    )

    if SKIP_EMPTY_BUSINESS_TYPE:
        empty_bt = out[out["business_type"].map(clean_spaces) == ""].copy()
        if not empty_bt.empty:
            empty_bt["reason"] = "empty_business_type"
            empty_bt["detail"] = (
                "SKIP_EMPTY_BUSINESS_TYPE=True and business_type empty (owner not found/empty)"
            )
            unacc_rows.append(empty_bt)
        out = out[out["business_type"].map(clean_spaces) != ""].copy()

    if ban_business_set:
        banned_bt = out[
            out["business_type"].map(clean_spaces).isin(ban_business_set)
        ].copy()
        if not banned_bt.empty:
            banned_bt["reason"] = "banned_business_type"
            banned_bt["detail"] = "business_type in BAN_BUSINESS_TYPES"
            unacc_rows.append(banned_bt)
        out = out[~out["business_type"].map(clean_spaces).isin(ban_business_set)].copy()

    if out.empty:
        die("Нет данных после фильтра по business_type (всё ушло в Unaccounted)")

    total = int(out["secrets"].sum()) or 1
    out["percent"] = (out["secrets"] / total) * 100

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

    unaccounted = (
        pd.concat(unacc_rows, ignore_index=True) if unacc_rows else pd.DataFrame()
    )
    if not unaccounted.empty:
        if "service_name" not in unaccounted.columns:
            unaccounted["service_name"] = ""
        if "owner_for_report" not in unaccounted.columns:
            unaccounted["owner_for_report"] = unaccounted.get("owner", "")
        if "business_type" not in unaccounted.columns:
            unaccounted["business_type"] = ""

        unaccounted = unaccounted.rename(
            columns={
                "service_name": "Наименование сервиса",
                "code": "КОД",
                "owner_for_report": "Владелец сервиса",
                "secrets": "Кол-во секретов",
                "business_type": "Тип бизнеса",
            }
        )

        # базовые колонки
        cols = [
            "kv",
            "КОД",
            "Наименование сервиса",
            "Владелец сервиса",
            "Тип бизнеса",
            "Кол-во секретов",
            "reason",
            "detail",
        ]
        for c in cols:
            if c not in unaccounted.columns:
                unaccounted[c] = ""
        unaccounted = unaccounted[cols].copy()

        unaccounted["__secrets_sort"] = (
            pd.to_numeric(unaccounted["Кол-во секретов"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        unaccounted = (
            unaccounted.sort_values("__secrets_sort", ascending=False)
            .drop(columns=["__secrets_sort"])
            .reset_index(drop=True)
        )

    with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="Отчет Vault")
        ws = writer.book["Отчет Vault"]
        for c in ws[1]:
            c.font = Font(bold=True)

        if not unaccounted.empty:
            unaccounted.to_excel(writer, index=False, sheet_name="Unaccounted")
            ws_u = writer.book["Unaccounted"]
            for c in ws_u[1]:
                c.font = Font(bold=True)

    log.info(
        "Итог: учтено строк=%d, unaccounted=%d, файл=%s",
        len(out),
        len(unaccounted),
        OUT_FILE,
    )
    log.info("==== DONE ====")


if __name__ == "__main__":
    main()
