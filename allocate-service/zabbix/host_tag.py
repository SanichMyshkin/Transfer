import os
import logging
import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("zbx_match")


def load_excel_assets(path):
    df = pd.read_excel(path, engine="openpyxl", dtype=str)

    raw = []
    for service, host, ip in zip(df.iloc[:, 0], df.iloc[:, 13], df.iloc[:, 21]):
        if not service or str(service).lower() == "nan":
            continue

        service = service.strip()
        excel_host = None if not host or str(host).lower() == "nan" else host.strip()
        excel_ip_raw = None if not ip or str(ip).lower() == "nan" else ip.strip()

        is_old = False
        excel_ip = excel_ip_raw
        if excel_ip_raw and "old" in excel_ip_raw.lower():
            is_old = True
            excel_ip = excel_ip_raw.split("-", 1)[0].strip()

        raw.append({
            "service": service,
            "excel_host": excel_host,
            "excel_ip_raw": excel_ip_raw,
            "excel_ip": excel_ip,
            "is_old": is_old,
        })

    non_old_ips = set()
    for r in raw:
        if r["excel_ip"] and not r["is_old"]:
            non_old_ips.add(r["excel_ip"])

    rows = []
    old_skipped = 0
    old_kept = 0
    for r in raw:
        if r["is_old"] and r["excel_ip"] and r["excel_ip"] in non_old_ips:
            old_skipped += 1
            continue
        if r["is_old"]:
            old_kept += 1
        rows.append(r)

    log.info("Загружено строк из Excel: %d", len(raw))
    if old_skipped:
        log.info("Пропущено строк с old из-за дублей: %d", old_skipped)
    if old_kept:
        log.info("Оставлено строк с old (единственные): %d", old_kept)

    return rows


def build_zabbix_maps(zabbix_hosts):
    host_by_id = {}
    ip_to_ids = {}
    dns_to_ids = {}

    for zh in zabbix_hosts:
        hid = str(zh["hostid"])
        host = (zh.get("host") or "").strip()
        name = (zh.get("name") or "").strip()
        ips = set()
        dns = set()

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

        for d in dns:
            dns_to_ids.setdefault(d, set()).add(hid)

        host_by_id[hid] = {
            "hostid": hid,
            "host": host,
            "ips": ips,
            "dns": dns,
        }

    log.info(
        "Построены индексы Zabbix: хостов=%d ip=%d dns=%d",
        len(host_by_id), len(ip_to_ids), len(dns_to_ids)
    )
    return host_by_id, ip_to_ids, dns_to_ids


def resolve_ip_by_hostname(ids, excel_host, host_by_id):
    eh = excel_host.lower()
    eh_short = eh.split(".", 1)[0] if "." in eh else None
    matched = []
    for hid in ids:
        zh = host_by_id[hid]
        if eh in zh["dns"] or (eh_short and eh_short in zh["dns"]):
            matched.append(hid)
    if len(matched) == 1:
        return matched[0], "уточнено по hostname"
    if len(matched) == 0:
        return None, "hostname не совпал ни с одним кандидатом"
    return None, "hostname совпал с несколькими кандидатами"


def match_all(zabbix_hosts, excel_rows):
    host_by_id, ip_to_ids, dns_to_ids = build_zabbix_maps(zabbix_hosts)

    matched = []
    matched_hostids = set()
    conflicts = []

    for row in excel_rows:
        service = row["service"]
        excel_host = row["excel_host"]
        excel_ip = row["excel_ip"]
        excel_ip_raw = row["excel_ip_raw"]
        is_old = row["is_old"]

        if is_old:
            log.warning("Запись имеет old: service=%s host=%s ip=%s", service, excel_host, excel_ip_raw)

        if excel_ip and excel_ip in ip_to_ids:
            ids = ip_to_ids[excel_ip]

            if len(ids) == 1:
                hid = next(iter(ids))
                zh = host_by_id[hid]
                matched.append({
                    "service": service,
                    "match_field": "ip_old" if is_old else "ip",
                    "excel_host": excel_host,
                    "excel_ip": excel_ip_raw,
                    "zabbix_host_name": zh["host"],
                    "zabbix_ip": excel_ip,
                })
                matched_hostids.add(hid)
                continue

            if excel_host:
                hid, reason = resolve_ip_by_hostname(ids, excel_host, host_by_id)
                if hid:
                    zh = host_by_id[hid]
                    log.warning(
                        "IP %s используется несколькими хостами, но сопоставлен по hostname %s → %s",
                        excel_ip, excel_host, zh["host"]
                    )
                    matched.append({
                        "service": service,
                        "match_field": "ip+hostname_old" if is_old else "ip+hostname",
                        "excel_host": excel_host,
                        "excel_ip": excel_ip_raw,
                        "zabbix_host_name": zh["host"],
                        "zabbix_ip": excel_ip,
                    })
                    matched_hostids.add(hid)
                    continue

                conflicts.append({
                    "type": "ambiguous_ip",
                    "service": service,
                    "excel_host": excel_host,
                    "excel_ip": excel_ip_raw,
                    "candidates": [host_by_id[i]["host"] for i in ids],
                    "reason": reason,
                })
                continue

            conflicts.append({
                "type": "ambiguous_ip",
                "service": service,
                "excel_host": excel_host,
                "excel_ip": excel_ip_raw,
                "candidates": [host_by_id[i]["host"] for i in ids],
                "reason": "ip неоднозначный, а excel_host пустой",
            })
            continue

        if excel_host:
            key = excel_host.lower()
            key_short = key.split(".", 1)[0] if "." in key else None
            ids = set()
            if key in dns_to_ids:
                ids |= dns_to_ids[key]
            if key_short and key_short in dns_to_ids:
                ids |= dns_to_ids[key_short]

            if len(ids) == 1:
                hid = next(iter(ids))
                zh = host_by_id[hid]
                ip_any = next(iter(zh["ips"]), None)
                matched.append({
                    "service": service,
                    "match_field": "hostname_old" if is_old else "hostname",
                    "excel_host": excel_host,
                    "excel_ip": excel_ip_raw,
                    "zabbix_host_name": zh["host"],
                    "zabbix_ip": ip_any,
                })
                matched_hostids.add(hid)
                continue

            if len(ids) > 1:
                conflicts.append({
                    "type": "ambiguous_hostname",
                    "service": service,
                    "excel_host": excel_host,
                    "excel_ip": excel_ip_raw,
                    "candidates": [host_by_id[i]["host"] for i in ids],
                })
                continue

        conflicts.append({
            "type": "not_found",
            "service": service,
            "excel_host": excel_host,
            "excel_ip": excel_ip_raw,
        })

    unmatched_zabbix = []
    for hid, zh in host_by_id.items():
        if hid in matched_hostids:
            continue
        ip_any = next(iter(zh["ips"]), None)
        dns_any = next(iter(zh["dns"]), None)
        unmatched_zabbix.append({
            "zabbix_host_name": zh["host"],
            "zabbix_ip": ip_any,
            "zabbix_dns": dns_any,
        })

    return matched, unmatched_zabbix, conflicts


def export_excel(matched, unmatched):
    with pd.ExcelWriter("match_results.xlsx", engine="openpyxl") as writer:
        pd.DataFrame(matched).to_excel(writer, sheet_name="Matched", index=False)
        pd.DataFrame(unmatched).to_excel(writer, sheet_name="Unmatched_Zabbix", index=False)
    log.info("Результаты сохранены в match_results.xlsx")


def main():
    load_dotenv()
    url = os.getenv("ZABBIX_URL")
    token = os.getenv("ZABBIX_TOKEN")

    if not url or not token:
        log.error("Не заданы ZABBIX_URL / ZABBIX_TOKEN")
        return

    excel_rows = load_excel_assets(r"tags\service_db.xlsx")

    try:
        api = ZabbixAPI(url=url)
        api.login(token=token)
        zabbix_hosts_all = api.host.get(
            output=["hostid", "host", "name", "status"],
            selectInterfaces=["ip", "dns"],
        )
        zabbix_hosts = [h for h in zabbix_hosts_all if str(h.get("status")) == "0"]
        log.info("Получено хостов из Zabbix: %d (активных=%d, отключённых=%d)",
                 len(zabbix_hosts_all), len(zabbix_hosts), len(zabbix_hosts_all) - len(zabbix_hosts))
    except Exception:
        log.exception("Ошибка работы с Zabbix API")
        return

    matched, unmatched, conflicts = match_all(zabbix_hosts, excel_rows)

    log.info(
        "ИТОГО: сопоставлено=%d нераспределённых_в_zabbix=%d конфликтов=%d",
        len(matched), len(unmatched), len(conflicts)
    )

    if conflicts:
        log.error("КОНФЛИКТЫ (первые 10):")
        for c in conflicts[:10]:
            log.error(c)

    export_excel(matched, unmatched)


if __name__ == "__main__":
    main()
