import os
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

OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "zabbix_report.xlsx")

BAN_SERVICE_IDS = [15473]
SKIP_EMPTY_SERVICE_ID = True

BAN_BUSINESS_TYPES = [
]
SKIP_EMPTY_BUSINESS_TYPE = True

ZBX_CHUNK = 500

logger = logging.getLogger("zabbix_report")
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

    if not DB_FILE:
        die("ENV DB_FILE пустой")
    if not os.path.isfile(DB_FILE):
        die(f"DB_FILE не найден: {DB_FILE}")

    if not SD_FILE:
        die("ENV SD_FILE пустой")
    if not os.path.isfile(SD_FILE):
        die(f"SD_FILE не найден: {SD_FILE}")

    if not BK_FILE:
        die("ENV BK_FILE пустой")
    if not os.path.isfile(BK_FILE):
        die(f"BK_FILE не найден: {BK_FILE}")

    out_dir = os.path.dirname(OUTPUT_XLSX)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)


def build_ban_set(ban_list):
    if not isinstance(ban_list, (list, tuple, set)):
        die("BAN_SERVICE_IDS должен быть list / tuple / set")
    return {str(x).strip() for x in ban_list if str(x).strip()}


def clean_spaces(s: str) -> str:
    s = (s or "").strip()
    s = s.replace(",", " ")
    s = " ".join(s.split())
    return s


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
        yield lst[i : i + n]


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
                "hostid": str(h.get("hostid", "")).strip(),
                "host": (h.get("host") or "").strip(),
                "ip": clean_ip_only_32(iface.get("ip")),
                "dns": clean_dns(iface.get("dns")),
            }
        )
    return pd.DataFrame(rows)


def fetch_items_triggers_counts(api, hostids):
    items_cnt = defaultdict(int)
    triggers_cnt = defaultdict(int)

    hostids = [str(x).strip() for x in hostids if str(x).strip()]
    if not hostids:
        return dict(items_cnt), dict(triggers_cnt)

    for part in chunks(hostids, ZBX_CHUNK):
        try:
            items = api.item.get(
                hostids=part,
                output=["itemid", "hostid", "status"],
            )
        except Exception as e:
            logger.error(f"item.get failed: {e}")
            items = []

        for it in items or []:
            if str(it.get("status", "0")) != "0":
                continue
            hid = str(it.get("hostid", "")).strip()
            if hid:
                items_cnt[hid] += 1

    for part in chunks(hostids, ZBX_CHUNK):
        try:
            trigs = api.trigger.get(
                hostids=part,
                output=["triggerid", "status"],
                selectHosts=["hostid"],
            )
        except Exception as e:
            logger.error(f"trigger.get failed: {e}")
            trigs = []

        for tr in trigs or []:
            if str(tr.get("status", "0")) != "0":
                continue
            for h in (tr.get("hosts") or []):
                hid = str(h.get("hostid", "")).strip()
                if hid:
                    triggers_cnt[hid] += 1

    return dict(items_cnt), dict(triggers_cnt)


def load_db(path):
    df = pd.read_excel(path, usecols="A,B,N,V", dtype=str, engine="openpyxl")
    df.columns = ["service", "service_id", "dns", "ip"]
    df = df.fillna("")

    df["service"] = df["service"].astype(str).map(clean_spaces)
    df["service_id"] = df["service_id"].astype(str).str.strip()
    df["ip"] = df["ip"].astype(str).map(clean_ip_only_32)
    df["dns"] = df["dns"].astype(str).map(clean_dns)

    return df


def load_sd_people_map(path):
    df = pd.read_excel(path, usecols="B,H", dtype=str, engine="openpyxl")
    df.columns = ["service_id", "owner"]
    df = df.fillna("")

    df["service_id"] = df["service_id"].astype(str).str.strip()
    df["owner"] = df["owner"].astype(str).map(clean_spaces)

    df = df[df["service_id"] != ""].copy()
    last = df.drop_duplicates("service_id", keep="last")

    return {
        sid: {"owner": o}
        for sid, o in zip(last["service_id"].tolist(), last["owner"].tolist())
    }


def load_bk_business_type_map(path):
    df = pd.read_excel(path, usecols="A:C,AS", dtype=str, engine="openpyxl")
    df = df.fillna("")
    df.columns = ["c1", "c2", "c3", "business_type"]

    def make_fio(r):
        fio = " ".join(
            [clean_spaces(r["c2"]), clean_spaces(r["c1"]), clean_spaces(r["c3"])]
        )
        return clean_spaces(fio)

    df["fio_key"] = df.apply(make_fio, axis=1).map(normalize_name_key)
    df["business_type"] = df["business_type"].astype(str).map(clean_spaces)

    df = df[df["fio_key"] != ""].copy()
    last = df.drop_duplicates("fio_key", keep="last")
    return dict(zip(last["fio_key"], last["business_type"]))


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
    logger.info("Старт формирования отчета Zabbix (hosts/items/triggers)")

    validate_env_and_files()

    ban_set = build_ban_set(BAN_SERVICE_IDS)
    ban_business_set = {clean_spaces(x) for x in BAN_BUSINESS_TYPES if clean_spaces(x)}

    api = ZabbixAPI(url=ZABBIX_URL)
    try:
        api.login(token=ZABBIX_TOKEN)

        df_hosts = fetch_hosts(api)
        total_active = len(df_hosts)

        items_cnt, triggers_cnt = fetch_items_triggers_counts(
            api, df_hosts["hostid"].tolist() if not df_hosts.empty else []
        )
    finally:
        try:
            api.logout()
        except Exception:
            pass

    if df_hosts.empty:
        die("В Zabbix не найдено активных хостов (status=0)")

    df_hosts["items"] = df_hosts["hostid"].map(lambda x: int(items_cnt.get(str(x), 0)))
    df_hosts["triggers"] = df_hosts["hostid"].map(
        lambda x: int(triggers_cnt.get(str(x), 0))
    )

    df_db = load_db(DB_FILE)
    sd_people_map = load_sd_people_map(SD_FILE)
    bk_type_map = load_bk_business_type_map(BK_FILE)

    map_both, dup_both = build_map(df_db, ["ip", "dns"])
    map_ip, dup_ip = build_map(df_db[df_db["ip"] != ""], ["ip"])
    map_dns, dup_dns = build_map(df_db[df_db["dns"] != ""], ["dns"])

    per_service = {}
    matched_hosts = 0
    ambiguous = 0
    banned_hits = 0
    skipped_empty = 0
    failed_rows = []

    for r in df_hosts.to_dict("records"):
        ip = r.get("ip", "")
        dns = r.get("dns", "")
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
            failed_rows.append(
                {
                    "Хост": r.get("host", ""),
                    "IP": ip,
                    "DNS": dns,
                    "Айтемы": int(r.get("items", 0)),
                    "Триггеры": int(r.get("triggers", 0)),
                }
            )
            continue

        service, service_id = svc_key

        if SKIP_EMPTY_SERVICE_ID and not service_id:
            skipped_empty += 1
            continue

        if service_id in ban_set:
            banned_hits += 1
            continue

        matched_hosts += 1
        if amb:
            ambiguous += 1

        key = (service, service_id)
        if key not in per_service:
            per_service[key] = {"hosts": 0, "items": 0, "triggers": 0}
        per_service[key]["hosts"] += 1
        per_service[key]["items"] += int(r.get("items", 0))
        per_service[key]["triggers"] += int(r.get("triggers", 0))

    candidates = []
    for (service, service_id), cnts in per_service.items():
        people = sd_people_map.get(service_id, {"owner": ""})
        owner = people.get("owner", "")
        business_type = bk_type_map.get(normalize_name_key(owner), "") if owner else ""
        business_type = clean_spaces(business_type)

        if SKIP_EMPTY_BUSINESS_TYPE and not business_type:
            continue
        if ban_business_set and business_type in ban_business_set:
            continue

        candidates.append(
            {
                "Тип бизнеса": business_type,
                "Наименование сервиса": service,
                "КОД": service_id,
                "Владелец сервиса": owner,
                "Кол-во хостов": int(cnts["hosts"]),
                "Кол-во айтемов": int(cnts["items"]),
                "Кол-во триггеров": int(cnts["triggers"]),
            }
        )

    eligible_items_total = sum(x["Кол-во айтемов"] for x in candidates)

    for x in candidates:
        pct = (
            (x["Кол-во айтемов"] / eligible_items_total) * 100
            if eligible_items_total
            else 0.0
        )
        x["% потребления (items)"] = round(pct, 2)

    df_report = pd.DataFrame(candidates).sort_values(
        ["Кол-во айтемов", "Кол-во хостов"],
        ascending=[False, False],
    )

    df_failed = pd.DataFrame(failed_rows)

    installed_pct = (matched_hosts / total_active * 100) if total_active else 0.0

    logger.info(f"Активных хостов: {total_active}")
    logger.info(
        f"Установлено/определено хостов: {matched_hosts} ({installed_pct:.2f}%)"
    )
    logger.info(f"Сомнительных совпадений: {ambiguous}")
    logger.info(f"Скип по бан-листу (service_id): {banned_hits}")
    logger.info(f"Скип пустых service_id: {skipped_empty}")
    logger.info(f"Итого айтемов (eligible): {eligible_items_total}")
    logger.info(f"Сервисов в отчёте: {len(df_report)}")
    logger.info(f"Хостов в 'Не установлены': {len(df_failed)}")

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df_report.to_excel(writer, index=False, sheet_name="Отчет Zabbix")
        df_failed.to_excel(writer, index=False, sheet_name="Не установлены")


if __name__ == "__main__":
    main()
