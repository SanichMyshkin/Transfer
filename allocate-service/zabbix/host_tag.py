import os
import sys
import json
import logging
from dotenv import load_dotenv
import pandas as pd
from zabbix_utils import ZabbixAPI

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("zbx_match")

STRICT = True  # если True — при конфликтах завершаем скрипт с кодом 1


def load_excel_assets(path: str):
    df = pd.read_excel(path, engine="openpyxl", dtype=str)

    assets = []
    for service, host, ip in zip(df.iloc[:, 0], df.iloc[:, 13], df.iloc[:, 21]):
        if not service or str(service).lower() == "nan":
            continue

        assets.append({
            "service": service.strip(),
            "excel_host": None if not host or str(host).lower() == "nan" else host.strip(),
            "excel_ip": None if not ip or str(ip).lower() == "nan" else ip.strip(),
        })

    log.info("Excel assets loaded: %d", len(assets))
    return assets


def build_zabbix_maps(zabbix_hosts):
    """
    Возвращает:
      host_by_id: hostid -> {"hostid","host","name","ips":set,"dns":set}
      ip_to_ids: ip -> set(hostid)
      dns_to_ids: dns_lower -> set(hostid)
    """
    host_by_id = {}
    ip_to_ids = {}
    dns_to_ids = {}

    for zh in zabbix_hosts:
        hid = str(zh.get("hostid"))
        host = (zh.get("host") or "").strip()
        name = (zh.get("name") or "").strip()

        ips = set()
        dns = set()

        # host/name тоже считаем как dns-ключи
        if host:
            dns.add(host.lower())
        if name:
            dns.add(name.lower())

        for iface in zh.get("interfaces", []) or []:
            ip = (iface.get("ip") or "").strip()
            d = (iface.get("dns") or "").strip()
            if ip:
                ips.add(ip)
                ip_to_ids.setdefault(ip, set()).add(hid)
            if d:
                dns.add(d.lower())
                dns_to_ids.setdefault(d.lower(), set()).add(hid)

        # добавить накопленные dns в индекс
        for d in dns:
            dns_to_ids.setdefault(d, set()).add(hid)

        host_by_id[hid] = {
            "hostid": hid,
            "host": host,
            "name": name,
            "ips": ips,
            "dns": dns,
        }

    log.info("Zabbix maps built: hosts=%d ip_keys=%d dns_keys=%d",
             len(host_by_id), len(ip_to_ids), len(dns_to_ids))
    return host_by_id, ip_to_ids, dns_to_ids


def get_candidates(row, ip_to_ids, dns_to_ids):
    """
    Возвращает (match_field, set(hostid), dns_variants_used)
    match_field: "ip" / "dns" / None
    """
    excel_ip = row.get("excel_ip")
    excel_host = row.get("excel_host")

    if excel_ip and excel_ip in ip_to_ids:
        return "ip", set(ip_to_ids[excel_ip]), []

    if excel_host:
        variants = [excel_host.lower()]
        if "." in excel_host:
            variants.append(excel_host.split(".", 1)[0].lower())

        # объединяем кандидатов по вариантам
        ids = set()
        used = []
        for v in variants:
            if v in dns_to_ids:
                ids |= dns_to_ids[v]
                used.append(v)

        if ids:
            return "dns", ids, used

    return None, set(), []


def match_all(zabbix_hosts, excel_rows):
    host_by_id, ip_to_ids, dns_to_ids = build_zabbix_maps(zabbix_hosts)

    matched = []
    conflicts = []  # сюда всё неоднозначное/противоречивое
    unmatched_excel = []

    # чтобы детектить “один zabbix host у разных сервисов”
    service_by_hostid = {}

    for row in excel_rows:
        service = row["service"]
        excel_host = row.get("excel_host")
        excel_ip = row.get("excel_ip")

        match_field, ids, used_variants = get_candidates(row, ip_to_ids, dns_to_ids)

        if not ids:
            unmatched_excel.append(row)
            continue

        if len(ids) > 1:
            conflicts.append({
                "type": "ambiguous_match",
                "service": service,
                "match_field": match_field,
                "excel_host": excel_host,
                "excel_ip": excel_ip,
                "dns_variants": used_variants,
                "candidates": [host_by_id[i]["host"] for i in sorted(ids)],
                "candidate_hostids": sorted(ids),
            })
            continue

        # ровно один кандидат
        hid = next(iter(ids))
        zh = host_by_id[hid]

        # cross-check: если совпали по IP, но excel_host есть и НЕ встречается среди dns-алиасов zabbix
        if match_field == "ip" and excel_host:
            eh = excel_host.lower()
            eh_short = excel_host.split(".", 1)[0].lower() if "." in excel_host else None
            if (eh not in zh["dns"]) and (eh_short and eh_short not in zh["dns"]):
                conflicts.append({
                    "type": "ip_host_mismatch",
                    "service": service,
                    "excel_host": excel_host,
                    "excel_ip": excel_ip,
                    "zabbix_host": zh["host"],
                    "zabbix_hostid": hid,
                    "zabbix_dns_sample": sorted(list(zh["dns"]))[:5],
                })
                # не продолжаем — это “подозрительное”, но связь по IP всё равно записать можно
                # если хочешь “жёстко” — перенеси в continue
                # continue

        # service conflict: один hid у разных service
        prev_service = service_by_hostid.get(hid)
        if prev_service and prev_service != service:
            conflicts.append({
                "type": "service_conflict",
                "zabbix_host": zh["host"],
                "zabbix_hostid": hid,
                "service_prev": prev_service,
                "service_new": service,
                "excel_host": excel_host,
                "excel_ip": excel_ip,
                "match_field": match_field,
            })
            continue

        service_by_hostid[hid] = service

        # zabbix ip для вывода: если матч по IP — это он, иначе возьмём любой ip (если есть)
        zabbix_ip = None
        if match_field == "ip":
            zabbix_ip = excel_ip
        else:
            zabbix_ip = next(iter(zh["ips"]), None)

        matched.append({
            "service": service,
            "match_field": match_field,
            "excel_host": excel_host,
            "excel_ip": excel_ip,
            "zabbix_host_name": zh["host"],
            "zabbix_ip": zabbix_ip,
        })

    # unmatched zabbix = те hostid, которые ни разу не попали в matched (и не лежат в конфликтных назначениях)
    matched_ids = set([m["zabbix_host_name"] for m in matched])
    unmatched_zabbix = []
    for hid, zh in host_by_id.items():
        if zh["host"] in matched_ids:
            continue
        ip = next(iter(zh["ips"]), None)
        # dns для отчёта можно брать любое (не обязательно)
        dns_any = next(iter(zh["dns"]), None)
        unmatched_zabbix.append({
            "zabbix_host_name": zh["host"],
            "zabbix_ip": ip,
            "zabbix_dns": dns_any,
        })

    return matched, unmatched_zabbix, conflicts, unmatched_excel


def export_to_excel(matched, unmatched_zabbix, out_xlsx="match_results.xlsx"):
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        pd.DataFrame(matched).to_excel(writer, sheet_name="Matched", index=False)
        pd.DataFrame(unmatched_zabbix).to_excel(writer, sheet_name="Unmatched_Zabbix", index=False)
    log.info("Exported: %s", out_xlsx)


def main():
    load_dotenv()
    ZABBIX_URL = os.getenv("ZABBIX_URL")
    ZABBIX_TOKEN = os.getenv("ZABBIX_TOKEN")
    if not ZABBIX_URL or not ZABBIX_TOKEN:
        log.error("ZABBIX_URL / ZABBIX_TOKEN not set")
        return 1

    excel_rows = load_excel_assets(r"tags\service_db.xlsx")

    try:
        api = ZabbixAPI(url=ZABBIX_URL)
        api.login(token=ZABBIX_TOKEN)
        zabbix_hosts = api.host.get(
            output=["hostid", "host", "name"],
            selectInterfaces=["ip", "dns"],
        )
        log.info("Zabbix hosts loaded: %d", len(zabbix_hosts))
    except Exception:
        log.exception("Zabbix API error")
        return 1

    matched, unmatched_zabbix, conflicts, unmatched_excel = match_all(zabbix_hosts, excel_rows)

    log.info("RESULT: matched=%d  unmatched_zabbix=%d  conflicts=%d  unmatched_excel=%d",
             len(matched), len(unmatched_zabbix), len(conflicts), len(unmatched_excel))

    # покажем примеры конфликтов (первые 10)
    if conflicts:
        log.error("CONFLICTS detected (showing up to 10):")
        for c in conflicts[:10]:
            log.error(json.dumps(c, ensure_ascii=False))

    export_to_excel(matched, unmatched_zabbix, "match_results.xlsx")

    # строгий режим: если есть конфликты или excel строки вообще не сопоставились — падаем
    if STRICT and (conflicts or unmatched_excel):
        log.error("STRICT mode: failing due to conflicts/unmatched_excel.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
