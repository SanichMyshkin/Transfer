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

BAN_SERVICE_IDS = [11111]
SKIP_EMPTY_SERVICE_ID = True

logger = logging.getLogger("zabbix_ownership")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
)
logger.handlers.clear()
logger.addHandler(handler)


def die(msg: str, code: int = 2):
    logger.error(msg)
    raise SystemExit(code)


def validate_env_and_files():
    if not ZABBIX_URL:
        die("ENV ZABBIX_URL пустой")
    if not ZABBIX_TOKEN:
        die("ENV ZABBIX_TOKEN пустой")

    if not DB_XLSX:
        die("ENV DB_XLSX пустой")
    if not os.path.isfile(DB_XLSX):
        die(f"DB_XLSX не найден: {DB_XLSX}")

    if not SD_XLSX:
        die("ENV SD_XLSX пустой")
    if not os.path.isfile(SD_XLSX):
        die(f"SD_XLSX не найден: {SD_XLSX}")

    out_dir = os.path.dirname(OUTPUT_XLSX)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)


def build_ban_set(ban_list):
    if not isinstance(ban_list, (list, tuple, set)):
        die("BAN_SERVICE_IDS должен быть list / tuple / set")
    return {str(x).strip() for x in ban_list if str(x).strip()}


ban_set = build_ban_set(BAN_SERVICE_IDS)


def clean_dns(s: str) -> str:
    return (s or "").strip().lower()


def clean_ip_only_32(s: str) -> str:
    s = (s or "").strip()
    if s.endswith("/32"):
        s = s[:-3].strip()
    return s


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
        if int(h.get("status", 0)) != 0:
            continue
        iface = pick_primary_interface(h.get("interfaces") or [])
        rows.append(
            {
                "ip": clean_ip_only_32(iface.get("ip")),
                "dns": clean_dns(iface.get("dns")),
            }
        )
    return pd.DataFrame(rows)


def load_db(path):
    df = pd.read_excel(path, usecols="A,B,N,V", dtype=str, engine="openpyxl")
    df.columns = ["service", "service_id", "dns", "ip"]
    df = df.fillna("")

    df["service"] = df["service"].astype(str).str.strip()
    df["service_id"] = df["service_id"].astype(str).str.strip()
    df["ip"] = df["ip"].astype(str).map(clean_ip_only_32)
    df["dns"] = df["dns"].astype(str).map(clean_dns)

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
    return dict(zip(last["service_id"], last["owner"]))


def build_map(df, keys):
    tmp = df.copy()
    tmp["_k"] = list(map(tuple, tmp[keys].to_numpy()))
    counts = tmp["_k"].value_counts().to_dict()
    last = tmp.drop_duplicates("_k", keep="last")
    mp = {
        k: (r["service"], r["service_id"])
        for k, r in zip(last["_k"], last.to_dict("records"))
    }
    return mp, counts


def main():
    validate_env_and_files()

    api = ZabbixAPI(url=ZABBIX_URL)
    try:
        api.login(token=ZABBIX_TOKEN)
        df_hosts = fetch_hosts(api)
    finally:
        try:
            api.logout()
        except Exception:
            pass

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
    skipped_empty = 0

    for ip, dns in df_hosts[["ip", "dns"]].itertuples(index=False, name=None):
        svc_key = None
        amb = False

        if ip and dns and (ip, dns) in map_both:
            svc_key = map_both[(ip, dns)]
            amb = dup_both.get((ip, dns), 0) > 1
        elif ip and (ip,) in map_ip:
            svc_key = map_ip[(ip,)]
            amb = dup_ip.get((ip,), 0) > 1
        elif dns and (dns,) in map_dns:
            svc_key = map_dns[(dns,)]
            amb = dup_dns.get((dns,), 0) > 1

        if not svc_key:
            continue

        service, service_id = svc_key

        if SKIP_EMPTY_SERVICE_ID and not service_id:
            skipped_empty += 1
            continue

        if service_id in ban_set:
            banned_hits += 1
            continue

        matched += 1
        if amb:
            ambiguous += 1

        per_service[(service, service_id)] = per_service.get(
            (service, service_id), 0
        ) + 1

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

    df_report = pd.DataFrame(
        rows, columns=["service", "service_id", "owner", "hosts_found", "percent"]
    )

    if not df_report.empty:
        df_report = df_report.sort_values(
            ["hosts_found", "service", "service_id"],
            ascending=[False, True, True],
        )

    logger.info(f"Активных хостов: {total_active}")
    logger.info(
        f"Определено хостов: {matched} "
        f"({(matched / total_active * 100) if total_active else 0:.2f}%)"
    )
    logger.info(f"Сомнительных совпадений: {ambiguous}")
    logger.info(f"Скип по бан-листу: {banned_hits}")
    logger.info(f"Скип пустых service_id: {skipped_empty}")
    logger.info(f"Сервисов в отчёте: {len(df_report)}")

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df_report.to_excel(writer, index=False, sheet_name="report")


if __name__ == "__main__":
    main()
