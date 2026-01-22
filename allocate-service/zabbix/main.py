import os
import logging

import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

load_dotenv()

ZABBIX_URL = os.getenv("ZABBIX_URL", "").rstrip("/")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN", "")

DB_XLSX = os.getenv("DB_XLSX", "db.xlsx").strip()
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "zabbix_report.xlsx")

logger = logging.getLogger("zabbix_hosts_ownership")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.handlers.clear()
logger.addHandler(handler)

if not ZABBIX_URL or not ZABBIX_TOKEN:
    logger.error("Не найден ZABBIX_URL или ZABBIX_TOKEN в .env")
    raise SystemExit(1)

if not DB_XLSX:
    logger.error("Не найден DB_XLSX в .env (путь к db.xlsx)")
    raise SystemExit(1)

if not os.path.exists(DB_XLSX):
    logger.error(f"DB_XLSX не существует: {DB_XLSX}")
    raise SystemExit(1)


def pick_primary_interface(interfaces):
    if not interfaces:
        return {}
    for it in interfaces:
        if str(it.get("main")) == "1":
            return it
    return interfaces[0]


def fetch_hosts_min(api):
    raw = api.host.get(
        output=["hostid", "host", "name", "status"],
        selectInterfaces=["ip", "dns", "main"],
    )

    rows = []
    for h in raw or []:
        iface = pick_primary_interface(h.get("interfaces") or [])
        ip = (iface.get("ip") or "").strip()
        dns = (iface.get("dns") or "").strip()

        status_raw = h.get("status", 0)
        status_int = int(status_raw) if str(status_raw).isdigit() else 0
        active = True if status_int == 0 else False  # 0=enabled, 1=disabled

        name = (h.get("name") or h.get("host") or "").strip()

        rows.append(
            {
                "name": name,
                "ip": ip,
                "dns": dns,
                "active": active,
            }
        )
    return pd.DataFrame(rows, columns=["name", "ip", "dns", "active"])


def norm_ip(s: str) -> str:
    return (s or "").strip()


def norm_dns(s: str) -> str:
    return (s or "").strip().lower()


def load_db(db_path: str) -> pd.DataFrame:
    # Берём строго нужные колонки: A(service), B(service_id), N(dns), V(ip)
    df = pd.read_excel(db_path, usecols="A,B,N,V", dtype=str, engine="openpyxl").copy()
    df.columns = ["service", "service_id", "dns", "ip"]

    df["service"] = df["service"].fillna("").astype(str).str.strip()
    df["service_id"] = df["service_id"].fillna("").astype(str).str.strip()
    df["ip"] = df["ip"].fillna("").astype(str).map(norm_ip)
    df["dns"] = df["dns"].fillna("").astype(str).map(norm_dns)

    # выкидываем совсем пустые строки, где нет ни ip ни dns и нет сервиса/айди
    df = df[~((df["ip"] == "") & (df["dns"] == "") & (df["service"] == "") & (df["service_id"] == ""))].copy()
    return df


def build_last_map(df: pd.DataFrame, key_cols):
    """
    Возвращает:
      - last_map: key -> (service, service_id)
      - dup_count_map: key -> сколько строк в db на этот ключ (для "сомнительных")
    """
    tmp = df.copy()
    tmp["_key"] = list(zip(*[tmp[c].tolist() for c in key_cols]))
    counts = tmp["_key"].value_counts().to_dict()

    # берём "последнюю" по порядку в файле
    last_rows = tmp.drop_duplicates(subset=["_key"], keep="last")
    last_map = {
        k: (row["service"], row["service_id"])
        for k, row in zip(last_rows["_key"].tolist(), last_rows.to_dict("records"))
    }
    return last_map, counts


def match_ownership(active_hosts_df: pd.DataFrame, db_df: pd.DataFrame):
    map_both, dup_both = build_last_map(db_df, ["ip", "dns"])
    map_ip, dup_ip = build_last_map(db_df[db_df["ip"] != ""], ["ip"])
    map_dns, dup_dns = build_last_map(db_df[db_df["dns"] != ""], ["dns"])

    total_active = len(active_hosts_df)
    determined_total = 0
    ambiguous_matches = 0

    per_service = {}  # (service, service_id) -> count

    matched_rows = []

    for _, h in active_hosts_df.iterrows():
        ip = norm_ip(h["ip"])
        dns = norm_dns(h["dns"])

        owner = None
        amb = False

        if ip and dns and (ip, dns) in map_both:
            owner = map_both[(ip, dns)]
            amb = dup_both.get((ip, dns), 0) > 1
        elif ip and (ip,) in map_ip:
            owner = map_ip[(ip,)]
            amb = dup_ip.get((ip,), 0) > 1
        elif dns and (dns,) in map_dns:
            owner = map_dns[(dns,)]
            amb = dup_dns.get((dns,), 0) > 1

        if not owner:
            continue

        service, service_id = owner
        determined_total += 1
        if amb:
            ambiguous_matches += 1

        key = (service, service_id)
        per_service[key] = per_service.get(key, 0) + 1

        matched_rows.append(
            {
                "name": h["name"],
                "ip": ip,
                "dns": dns,
                "service": service,
                "service_id": service_id,
                "ambiguous": amb,
            }
        )

    return per_service, determined_total, total_active, ambiguous_matches, pd.DataFrame(matched_rows)


def main():
    logger.info("Подключаюсь к Zabbix...")
    api = ZabbixAPI(url=ZABBIX_URL)
    api.login(token=ZABBIX_TOKEN)
    logger.info("Ок")

    logger.info("Получаю хосты (name/ip/dns/active)...")
    df_hosts = fetch_hosts_min(api)
    api.logout()
    logger.info("Сессия закрыта")

    total_hosts = len(df_hosts)
    df_active = df_hosts[df_hosts["active"] == True].copy()
    logger.info(f"Хостов всего: {total_hosts}, активных: {len(df_active)} (выключенные скипаем)")

    logger.info(f"Читаю базу сопоставления: {DB_XLSX}")
    df_db = load_db(DB_XLSX)
    logger.info(f"Строк в db (A,B,N,V): {len(df_db)}")

    logger.info("Матчу ownership (ip+dns -> ip -> dns)...")
    per_service, determined_total, total_active, ambiguous_matches, df_matched = match_ownership(df_active, df_db)

    if total_active == 0:
        logger.info("В Zabbix нет активных хостов — нечего матчить")
        determined_pct_all_active = 0.0
    else:
        determined_pct_all_active = (determined_total / total_active) * 100.0

    logger.info(
        f"Определено (matched) активных хостов: {determined_total}/{total_active} "
        f"({determined_pct_all_active:.2f}%)"
    )
    logger.info(f"Сомнительных матчей (ключ в db встречался >1 раза, взяли последний): {ambiguous_matches}")

    report_rows = []
    for (service, service_id), cnt in per_service.items():
        pct_of_determined = (cnt / determined_total) * 100.0 if determined_total else 0.0
        report_rows.append(
            {
                "service": service,
                "service_id": service_id,
                "hosts_found": cnt,
                "percent_of_determined": round(pct_of_determined, 2),
            }
        )

    df_report = pd.DataFrame(report_rows, columns=["service", "service_id", "hosts_found", "percent_of_determined"])
    if not df_report.empty:
        df_report = df_report.sort_values(by=["hosts_found", "service", "service_id"], ascending=[False, True, True])

    logger.info(f"Пишу Excel: {OUTPUT_XLSX}")
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df_active.to_excel(writer, index=False, sheet_name="active_hosts")
        df_report.to_excel(writer, index=False, sheet_name="services_report")
        df_matched.to_excel(writer, index=False, sheet_name="matched_details")

    logger.info("Готово")
    return df_report


if __name__ == "__main__":
    df_report = main()
    print(df_report)
