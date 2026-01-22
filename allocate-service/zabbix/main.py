import os
import logging
import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

load_dotenv()

ZABBIX_URL = os.getenv("ZABBIX_URL", "").rstrip("/")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN", "")
DB_XLSX = os.getenv("DB_XLSX", "")
SD_XLSX = os.getenv("SD_XLSX", "")
OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "report.xlsx")
BAN_SERVICE_IDS = os.getenv("BAN_SERVICE_IDS", "")

logger = logging.getLogger("zabbix_ownership")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.handlers.clear()
logger.addHandler(handler)

if not ZABBIX_URL or not ZABBIX_TOKEN or not DB_XLSX or not SD_XLSX:
    raise SystemExit(1)

if not os.path.exists(DB_XLSX) or not os.path.exists(SD_XLSX):
    raise SystemExit(1)

ban_set = {x.strip() for x in BAN_SERVICE_IDS.split(",") if x.strip()}


def pick_primary_interface(interfaces):
    if not interfaces:
        return {}
    for it in interfaces:
        if str(it.get("main")) == "1":
            return it
    return interfaces[0]


def fetch_hosts(api):
    raw = api.host.get(
        output=["hostid", "host", "name", "status"],
        selectInterfaces=["ip", "dns", "main"],
    )
    rows = []
    for h in raw or []:
        status = int(h.get("status", 0))
        if status != 0:
            continue
        iface = pick_primary_interface(h.get("interfaces") or [])
        rows.append(
            {
                "ip": (iface.get("ip") or "").strip(),
                "dns": (iface.get("dns") or "").strip().lower(),
            }
        )
    return pd.DataFrame(rows)


def load_db(path):
    df = pd.read_excel(path, usecols="A,B,N,V", dtype=str, engine="openpyxl")
    df.columns = ["service", "service_id", "dns", "ip"]
    df = df.fillna("")
    df["service"] = df["service"].astype(str).str.strip()
    df["service_id"] = df["service_id"].astype(str).str.strip()
    df["ip"] = df["ip"].astype(str).str.strip()
    df["dns"] = df["dns"].astype(str).str.strip().str.lower()
    return df


def clean_owner(s):
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


def load_sd_owner_map(path):
    df = pd.read_excel(path, usecols="B,H", dtype=str, engine="openpyxl")
    df.columns = ["service_id", "owner"]
    df = df.fillna("")
    df["service_id"] = df["service_id"].astype(str).str.strip()
    df["owner"] = df["owner"].astype(str).map(clean_owner)
    df = df[df["service_id"] != ""].copy()
    last = df.drop_duplicates("service_id", keep="last")
    return {k: v for k, v in zip(last["service_id"].tolist(), last["owner"].tolist())}


def build_map(df, keys):
    tmp = df.copy()
    tmp["_k"] = list(zip(*[tmp[k].tolist() for k in keys]))
    counts = tmp["_k"].value_counts().to_dict()
    last = tmp.drop_duplicates("_k", keep="last")
    mp = {k: (r["service"], r["service_id"]) for k, r in zip(last["_k"], last.to_dict("records"))}
    return mp, counts


def main():
    api = ZabbixAPI(url=ZABBIX_URL)
    api.login(token=ZABBIX_TOKEN)
    df_hosts = fetch_hosts(api)
    api.logout()

    total_active = len(df_hosts)

    df_db = load_db(DB_XLSX)
    owner_map = load_sd_owner_map(SD_XLSX)

    map_both, dup_both = build_map(df_db, ["ip", "dns"])
    map_ip, dup_ip = build_map(df_db[df_db["ip"] != ""], ["ip"])
    map_dns, dup_dns = build_map(df_db[df_db["dns"] != ""], ["dns"])

    per_service = {}
    matched = 0
    ambiguous = 0
    banned_hits = 0

    for _, h in df_hosts.iterrows():
        ip = h["ip"]
        dns = h["dns"]

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
        if service_id in ban_set:
            banned_hits += 1
            continue

        matched += 1
        if amb:
            ambiguous += 1

        per_service[(service, service_id)] = per_service.get((service, service_id), 0) + 1

    rows = []
    for (service, service_id), cnt in per_service.items():
        pct = (cnt / matched) * 100 if matched else 0
        rows.append(
            {
                "service": service,
                "service_id": service_id,
                "owner": owner_map.get(service_id, ""),
                "hosts_found": cnt,
                "percent": round(pct, 2),
            }
        )

    df_report = pd.DataFrame(rows, columns=["service", "service_id", "owner", "hosts_found", "percent"])
    if not df_report.empty:
        df_report = df_report.sort_values(["hosts_found", "service", "service_id"], ascending=[False, True, True])

    logger.info(f"Активных хостов: {total_active}")
    logger.info(f"Определено хостов: {matched} ({(matched / total_active * 100) if total_active else 0:.2f}%)")
    logger.info(f"Сомнительных совпадений: {ambiguous}")
    logger.info(f"Скип по бан-листу (service_id): {banned_hits}")

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df_report.to_excel(writer, index=False, sheet_name="report")


if __name__ == "__main__":
    main()
