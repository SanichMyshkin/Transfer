import os
import time
import logging
from collections import defaultdict

import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

load_dotenv()

ZABBIX_URL = os.getenv("ZABBIX_URL", "").rstrip("/")
ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN", "")

DB_FILE = os.getenv("DB_FILE")
SD_FILE = os.getenv("SD_FILE")
BK_FILE = os.getenv("BK_FILE")

OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "zabbix_events_report.xlsx")

EVENT_TAG_NAME = (os.getenv("EVENT_TAG_NAME") or "").strip()
EVENT_DAYS = int(os.getenv("EVENT_DAYS", "350"))

BAN_SERVICE_IDS = [15473]
SKIP_EMPTY_SERVICE_ID = True

BAN_BUSINESS_TYPES = []
SKIP_EMPTY_BUSINESS_TYPE = True

ZBX_CHUNK = 200

logger = logging.getLogger("zabbix_events_report")
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

    if not DB_FILE or not os.path.isfile(DB_FILE):
        die(f"DB_FILE не найден: {DB_FILE}")
    if not SD_FILE or not os.path.isfile(SD_FILE):
        die(f"SD_FILE не найден: {SD_FILE}")
    if not BK_FILE or not os.path.isfile(BK_FILE):
        die(f"BK_FILE не найден: {BK_FILE}")

    if not EVENT_TAG_NAME:
        die("ENV EVENT_TAG_NAME пустой")

    out_dir = os.path.dirname(OUTPUT_XLSX)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)


def build_ban_set(ban_list):
    return {str(x).strip() for x in ban_list if str(x).strip()}


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    return " ".join(s.split())


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


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


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def fetch_active_hosts(api):
    raw = api.host.get(
        output=["hostid", "host", "status"],
        selectInterfaces=["ip", "dns", "main"],
    )

    rows = []
    for h in raw or []:
        if int(h.get("status", 0)) != 0:
            continue
        iface = pick_primary_interface(h.get("interfaces") or [])
        rows.append(
            {
                "hostid": str(h.get("hostid", "")).strip(),
                "host": (h.get("host") or "").strip(),
                "ip": clean_ip_only_32(iface.get("ip")),
                "dns": clean_dns(iface.get("dns")),
            }
        )

    return pd.DataFrame(rows)


def fetch_trigger_ids_by_tag(api, hostids):
    trig_ids = set()

    for part in chunks(hostids, ZBX_CHUNK):
        trigs = api.trigger.get(
            hostids=part,
            output=["triggerid", "status"],
            tags=[{"tag": EVENT_TAG_NAME}],  # только имя
        )
        for t in trigs or []:
            if str(t.get("status", "0")) != "0":
                continue
            tid = str(t.get("triggerid", "")).strip()
            if tid:
                trig_ids.add(tid)

    return sorted(trig_ids)


def fetch_events_count_by_host(api, triggerids, time_from, time_till):
    host_events = defaultdict(int)

    for part in chunks(triggerids, ZBX_CHUNK):
        evs = api.event.get(
            object=0,  # trigger events
            objectids=part,
            output=["eventid", "clock"],
            selectHosts=["hostid"],
            time_from=time_from,
            time_till=time_till,
        )

        for e in evs or []:
            for h in (e.get("hosts") or []):
                hid = str(h.get("hostid", "")).strip()
                if hid:
                    host_events[hid] += 1

    return dict(host_events)


def load_db(path):
    df = pd.read_excel(path, usecols="A,B,N,V", dtype=str, engine="openpyxl")
    df.columns = ["service", "service_id", "dns", "ip"]
    df = df.fillna("")

    df["service"] = df["service"].map(clean_spaces)
    df["service_id"] = df["service_id"].str.strip()
    df["ip"] = df["ip"].map(clean_ip_only_32)
    df["dns"] = df["dns"].map(clean_dns)

    return df


def load_sd_people_map(path):
    df = pd.read_excel(path, usecols="B,H", dtype=str, engine="openpyxl")
    df.columns = ["service_id", "owner"]
    df = df.fillna("")
    df["service_id"] = df["service_id"].str.strip()
    df["owner"] = df["owner"].map(clean_spaces)
    df = df[df["service_id"] != ""]
    last = df.drop_duplicates("service_id", keep="last")
    return dict(zip(last["service_id"], last["owner"]))


def load_bk_business_type_map(path):
    df = pd.read_excel(path, usecols="A:C,AS", dtype=str, engine="openpyxl")
    df = df.fillna("")
    df.columns = ["c1", "c2", "c3", "business_type"]

    df["fio_key"] = (
        df["c2"].map(clean_spaces) + " " +
        df["c1"].map(clean_spaces) + " " +
        df["c3"].map(clean_spaces)
    ).map(normalize_name_key)

    df["business_type"] = df["business_type"].map(clean_spaces)
    df = df[df["fio_key"] != ""]
    last = df.drop_duplicates("fio_key", keep="last")
    return dict(zip(last["fio_key"], last["business_type"]))


def build_map(df, keys):
    tmp = df.copy()
    tmp["_k"] = list(map(tuple, tmp[keys].to_numpy()))
    last = tmp.drop_duplicates("_k", keep="last")
    return {k: (r["service"], r["service_id"])
            for k, r in zip(last["_k"], last.to_dict("records"))}


def main():
    logger.info("Старт формирования отчета: события по тегу триггера")

    validate_env_and_files()

    ban_set = build_ban_set(BAN_SERVICE_IDS)
    ban_business_set = {clean_spaces(x) for x in BAN_BUSINESS_TYPES}

    now = int(time.time())
    time_from = now - EVENT_DAYS * 86400
    time_till = now

    api = ZabbixAPI(url=ZABBIX_URL)
    api.login(token=ZABBIX_TOKEN)

    df_hosts = fetch_active_hosts(api)
    if df_hosts.empty:
        die("Нет активных хостов")

    triggerids = fetch_trigger_ids_by_tag(api, df_hosts["hostid"].tolist())
    if not triggerids:
        die("Нет триггеров с указанным тегом")

    host_events = fetch_events_count_by_host(api, triggerids, time_from, time_till)

    api.logout()

    df_hosts["events"] = df_hosts["hostid"].map(lambda x: host_events.get(str(x), 0))

    df_db = load_db(DB_FILE)
    sd_map = load_sd_people_map(SD_FILE)
    bk_map = load_bk_business_type_map(BK_FILE)

    map_both = build_map(df_db, ["ip", "dns"])
    map_ip = build_map(df_db[df_db["ip"] != ""], ["ip"])
    map_dns = build_map(df_db[df_db["dns"] != ""], ["dns"])

    per_service = defaultdict(int)
    failed = []

    for r in df_hosts.to_dict("records"):
        ip, dns, evc = r["ip"], r["dns"], r["events"]
        svc_key = None

        if ip and dns and (ip, dns) in map_both:
            svc_key = map_both[(ip, dns)]
        elif ip and (ip,) in map_ip:
            svc_key = map_ip[(ip,)]
        elif dns and (dns,) in map_dns:
            svc_key = map_dns[(dns,)]

        if not svc_key:
            failed.append({"Хост": r["host"], "IP": ip, "DNS": dns, "События": evc})
            continue

        service, service_id = svc_key
        if SKIP_EMPTY_SERVICE_ID and not service_id:
            continue
        if service_id in ban_set:
            continue

        per_service[(service, service_id)] += evc

    rows = []
    for (service, service_id), ev_sum in per_service.items():
        owner = sd_map.get(service_id, "")
        bt = bk_map.get(normalize_name_key(owner), "")
        bt = clean_spaces(bt)

        if SKIP_EMPTY_BUSINESS_TYPE and not bt:
            continue
        if bt in ban_business_set:
            continue

        rows.append({
            "Тип бизнеса": bt,
            "Наименование сервиса": service,
            "КОД": service_id,
            "Владелец сервиса": owner,
            "События": ev_sum,
        })

    total_events = sum(x["События"] for x in rows)
    for x in rows:
        x["% потребления (events)"] = round(
            (x["События"] / total_events * 100) if total_events else 0.0,
            2
        )

    df_report = pd.DataFrame(rows).sort_values(
        ["События", "Наименование сервиса"],
        ascending=[False, True],
    )

    df_failed = pd.DataFrame(failed)

    logger.info(f"Активных хостов: {len(df_hosts)}")
    logger.info(f"Итого событий: {total_events}")
    logger.info(f"Сервисов в отчёте: {len(df_report)}")
    logger.info(f"Не сматченных хостов: {len(df_failed)}")

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df_report.to_excel(writer, index=False, sheet_name="Отчет Zabbix")
        df_failed.to_excel(writer, index=False, sheet_name="Не установлены")


if __name__ == "__main__":
    main()
