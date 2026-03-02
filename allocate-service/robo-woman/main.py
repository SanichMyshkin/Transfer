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

OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "robo-dama_report.xlsx")

TRIGGER_TAGS = [
    "robo_dama",
    "robo_dama_a",
]

EVENT_DAYS = 30
BAN_SERVICE_IDS = [15473]
SKIP_EMPTY_SERVICE_ID = True

BAN_BUSINESS_TYPES = []
SKIP_EMPTY_BUSINESS_TYPE = True
ZBX_CHUNK = 200

MAX_TRIGGERIDS_IN_CELL = 200

logger = logging.getLogger("zabbix_events_report")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
)
logger.handlers.clear()
logger.addHandler(handler)


def die(msg: str):
    logger.error(msg)
    raise SystemExit(2)


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def clean_spaces(s):
    s = (s or "").strip()
    return " ".join(s.replace(",", " ").split())


def normalize_name_key(s: str) -> str:
    return clean_spaces(s).lower()


def clean_dns(s):
    return (s or "").strip().lower()


def clean_ip_only_32(s):
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


def fetch_triggers_by_tags(api, hostids):
    triggerids_all = set()
    host_trigger_ids = defaultdict(set)
    host_tags_found = defaultdict(set)
    for tag in TRIGGER_TAGS:
        logger.info(f"Получаем триггеры по тегу: {tag}")
        for part in chunks(hostids, ZBX_CHUNK):
            trigs = api.trigger.get(
                hostids=part,
                output=["triggerid", "status"],
                tags=[{"tag": tag}],
                selectHosts=["hostid"],
            )
            for t in trigs or []:
                if str(t.get("status", "0")) != "0":
                    continue
                tid = str(t.get("triggerid", "")).strip()
                if not tid:
                    continue
                triggerids_all.add(tid)
                for h in t.get("hosts") or []:
                    hid = str(h.get("hostid", "")).strip()
                    if hid:
                        host_trigger_ids[hid].add(tid)
                        host_tags_found[hid].add(tag)
    return sorted(triggerids_all), host_trigger_ids, host_tags_found


def fetch_events_count_by_host(api, triggerids, time_from, time_till):
    host_events = defaultdict(int)
    for part in chunks(triggerids, ZBX_CHUNK):
        evs = api.event.get(
            object=0,
            objectids=part,
            output=["eventid"],
            selectHosts=["hostid"],
            time_from=time_from,
            time_till=time_till,
        )
        for e in evs or []:
            for h in e.get("hosts") or []:
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
    df["service_id"] = df["service_id"].astype(str).str.strip()
    df["owner"] = df["owner"].astype(str).map(clean_spaces)
    df = df[df["service_id"] != ""].copy()
    last = df.drop_duplicates("service_id", keep="last")
    return dict(zip(last["service_id"], last["owner"]))


def load_bk_business_type_map(path):
    df = pd.read_excel(path, usecols="A:C,AS", dtype=str, engine="openpyxl")
    df = df.fillna("")
    df.columns = ["c1", "c2", "c3", "business_type"]
    fio = (
        df["c2"].map(clean_spaces)
        + " "
        + df["c1"].map(clean_spaces)
        + " "
        + df["c3"].map(clean_spaces)
    )
    df["fio_key"] = fio.map(normalize_name_key)
    df["business_type"] = df["business_type"].map(clean_spaces)
    df = df[df["fio_key"] != ""].copy()
    last = df.drop_duplicates("fio_key", keep="last")
    return dict(zip(last["fio_key"], last["business_type"]))


def build_map(df, keys):
    tmp = df.copy()
    tmp["_k"] = list(map(tuple, tmp[keys].to_numpy()))
    last = tmp.drop_duplicates("_k", keep="last")
    return {
        k: (r["service"], r["service_id"])
        for k, r in zip(last["_k"], last.to_dict("records"))
    }


def main():
    logger.info("Старт отчета: tagged triggers + events + mapping")
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
    if not TRIGGER_TAGS:
        die("TRIGGER_TAGS пуст")
    now = int(time.time())
    time_from = now - EVENT_DAYS * 86400
    time_till = now
    ban_set = {str(x).strip() for x in BAN_SERVICE_IDS if str(x).strip()}
    ban_business_set = {clean_spaces(x) for x in BAN_BUSINESS_TYPES if clean_spaces(x)}
    api = ZabbixAPI(url=ZABBIX_URL)
    api.login(token=ZABBIX_TOKEN)
    df_hosts = fetch_active_hosts(api)
    if df_hosts.empty:
        die("Нет активных хостов")
    hostids = df_hosts["hostid"].tolist()
    logger.info(f"Активных хостов: {len(hostids)}")
    triggerids_all, host_trigger_ids, host_tags_found = fetch_triggers_by_tags(
        api, hostids
    )
    logger.info(f"Уникальных триггеров по всем тегам: {len(triggerids_all)}")
    if triggerids_all:
        host_events = fetch_events_count_by_host(
            api, triggerids_all, time_from, time_till
        )
    else:
        host_events = {}
    api.logout()
    df_hosts["triggers"] = df_hosts["hostid"].map(
        lambda x: len(host_trigger_ids.get(str(x), set()))
    )
    df_hosts["events"] = df_hosts["hostid"].map(
        lambda x: int(host_events.get(str(x), 0))
    )
    df_db = load_db(DB_FILE)
    sd_map = load_sd_people_map(SD_FILE)
    bk_map = load_bk_business_type_map(BK_FILE)
    map_both = build_map(df_db, ["ip", "dns"])
    map_ip = build_map(df_db[df_db["ip"] != ""], ["ip"])
    map_dns = build_map(df_db[df_db["dns"] != ""], ["dns"])
    per_service_events = defaultdict(int)
    per_service_triggers = defaultdict(int)
    unaccounted = []
    df_tagged_hosts = df_hosts[df_hosts["triggers"] > 0].copy()
    for r in df_tagged_hosts.to_dict("records"):
        ip = r["ip"]
        dns = r["dns"]
        trg = int(r["triggers"])
        evc = int(r["events"])
        svc_key = None
        if ip and dns and (ip, dns) in map_both:
            svc_key = map_both[(ip, dns)]
        elif ip and (ip,) in map_ip:
            svc_key = map_ip[(ip,)]
        elif dns and (dns,) in map_dns:
            svc_key = map_dns[(dns,)]
        if not svc_key:
            unaccounted.append(
                {
                    "reason": "no_mapping",
                    "Хост": r["host"],
                    "DNS": dns,
                    "IP": ip,
                    "Кол-во триггеров": trg,
                    "События": evc,
                }
            )
            continue
        service, service_id = svc_key
        service_id = str(service_id).strip()
        if SKIP_EMPTY_SERVICE_ID and not service_id:
            unaccounted.append(
                {
                    "reason": "empty_service_id",
                    "Хост": r["host"],
                    "DNS": dns,
                    "IP": ip,
                    "Кол-во триггеров": trg,
                    "События": evc,
                }
            )
            continue
        if service_id in ban_set:
            unaccounted.append(
                {
                    "reason": "banned_service_id",
                    "Хост": r["host"],
                    "DNS": dns,
                    "IP": ip,
                    "Кол-во триггеров": trg,
                    "События": evc,
                }
            )
            continue
        owner = sd_map.get(service_id, "")
        business_type = clean_spaces(bk_map.get(normalize_name_key(owner), ""))
        if SKIP_EMPTY_BUSINESS_TYPE and not business_type:
            unaccounted.append(
                {
                    "reason": "empty_business_type",
                    "Хост": r["host"],
                    "DNS": dns,
                    "IP": ip,
                    "Кол-во триггеров": trg,
                    "События": evc,
                }
            )
            continue
        if ban_business_set and business_type in ban_business_set:
            unaccounted.append(
                {
                    "reason": "banned_business_type",
                    "Хост": r["host"],
                    "DNS": dns,
                    "IP": ip,
                    "Кол-во триггеров": trg,
                    "События": evc,
                }
            )
            continue
        per_service_events[(business_type, service, service_id, owner)] += evc
        per_service_triggers[(business_type, service, service_id, owner)] += trg
    rows = []
    for key, trg_sum in per_service_triggers.items():
        business_type, service, service_id, owner = key
        ev_sum = per_service_events[key]
        rows.append(
            {
                "Тип бизнеса": business_type,
                "Наименование сервиса": service,
                "КОД": service_id,
                "Владелец сервиса": owner,
                "Кол-во триггеров": int(trg_sum),
                "События": int(ev_sum),
            }
        )
    total_events = sum(x["События"] for x in rows)
    for x in rows:
        x["% потребления (events)"] = (
            round(x["События"] / total_events, 4) if total_events else 0.0
        )
    df_report = pd.DataFrame(rows).sort_values(
        ["События", "Кол-во триггеров"],
        ascending=[False, False],
    )
    df_unaccounted = pd.DataFrame(unaccounted)
    logger.info(f"Итого событий (в отчете): {total_events}")
    logger.info(f"Сервисов в отчете: {len(df_report)}")
    logger.info(f"Unaccounted rows: {len(df_unaccounted)}")
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df_report.to_excel(writer, index=False, sheet_name="Отчет Zabbix")
        df_unaccounted.to_excel(writer, index=False, sheet_name="Unaccounted")


if __name__ == "__main__":
    main()
