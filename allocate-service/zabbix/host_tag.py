import os
import logging
import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("zbx_match")


def load_excel_rows(path):
    df = pd.read_excel(path, engine="openpyxl", dtype=str)
    rows = []
    for service, host, ip in zip(df.iloc[:, 0], df.iloc[:, 13], df.iloc[:, 21]):
        if not service or str(service).lower() == "nan":
            continue
        service = service.strip()
        excel_host = None if not host or str(host).lower() == "nan" else host.strip()
        ip_raw = None if not ip or str(ip).lower() == "nan" else ip.strip()

        is_old = False
        ip_clean = ip_raw
        if ip_raw and "old" in ip_raw.lower():
            is_old = True
            ip_clean = ip_raw.split("-", 1)[0].strip()

        rows.append(
            {
                "service": service,
                "excel_host": excel_host,
                "excel_ip_raw": ip_raw,
                "excel_ip": ip_clean,
                "is_old": is_old,
            }
        )
    log.info("Загружено строк из Excel: %d", len(rows))
    return rows


def filter_old_rows(rows):
    non_old_ips = set()
    for r in rows:
        if r["excel_ip"] and not r["is_old"]:
            non_old_ips.add(r["excel_ip"])

    out = []
    skipped = 0
    kept_old = 0
    for r in rows:
        if r["is_old"] and r["excel_ip"] and r["excel_ip"] in non_old_ips:
            skipped += 1
            continue
        if r["is_old"]:
            kept_old += 1
        out.append(r)

    if skipped:
        log.info("Строк с old пропущено из-за дублей по IP: %d", skipped)
    if kept_old:
        log.warning("Строк с old оставлено (единственные по IP): %d", kept_old)

    return out


def build_excel_indexes(rows):
    ip_to_entries = {}
    host_to_entries = {}

    for r in rows:
        svc = r["service"]
        h = r["excel_host"]
        ip = r["excel_ip"]

        entry = {
            "service": svc,
            "excel_host": h,
            "excel_ip_raw": r["excel_ip_raw"],
            "excel_ip": ip,
            "is_old": r["is_old"],
        }

        if ip:
            ip_to_entries.setdefault(ip, []).append(entry)

        if h:
            hl = h.lower()
            host_to_entries.setdefault(hl, []).append(entry)
            if "." in hl:
                host_to_entries.setdefault(hl.split(".", 1)[0], []).append(entry)

    return ip_to_entries, host_to_entries


def get_zabbix_keys(z):
    ips = []
    dns = []

    for iface in z.get("interfaces", []) or []:
        ip = (iface.get("ip") or "").strip()
        d = (iface.get("dns") or "").strip()
        if ip:
            ips.append(ip)
        if d:
            dl = d.lower()
            dns.append(dl)
            if "." in dl:
                dns.append(dl.split(".", 1)[0])

    host = (z.get("host") or "").strip()
    name = (z.get("name") or "").strip()

    if host:
        hl = host.lower()
        dns.append(hl)
        if "." in hl:
            dns.append(hl.split(".", 1)[0])

    if name:
        nl = name.lower()
        dns.append(nl)
        if "." in nl:
            dns.append(nl.split(".", 1)[0])

    ips = list(dict.fromkeys([x for x in ips if x]))
    dns = list(dict.fromkeys([x for x in dns if x]))
    return ips, set(dns)


def resolve_by_hostname(candidates, z_dns):
    filtered = []
    for c in candidates:
        eh = c.get("excel_host")
        if not eh:
            continue
        ehl = eh.lower()
        if ehl in z_dns or (("." in ehl) and ehl.split(".", 1)[0] in z_dns):
            filtered.append(c)

    services = sorted(set(x["service"] for x in candidates))

    if len(filtered) == 1:
        return filtered[0], "ip+hostname", None

    if len(filtered) > 1:
        services2 = sorted(set(x["service"] for x in filtered))
        return None, None, f"по hostname подходит несколько записей Excel: {services2}"

    return None, None, f"по IP несколько сервисов {services}, hostname не уточнил"


def assign_services_to_zabbix(zabbix_hosts, excel_rows):
    excel_rows = filter_old_rows(excel_rows)
    ip_to_entries, _ = build_excel_indexes(excel_rows)

    matched = []
    unmatched_zabbix = []
    conflicts = []

    warned_old_ips = set()

    for z in zabbix_hosts:
        hid = str(z.get("hostid"))
        z_host = z.get("host")
        z_ips, z_dns = get_zabbix_keys(z)

        candidates = []
        for ip in z_ips:
            if ip in ip_to_entries:
                candidates.extend(ip_to_entries[ip])

        if not candidates:
            ip_any = z_ips[0] if z_ips else None
            dns_any = next(iter(z_dns), None)
            unmatched_zabbix.append(
                {
                    "zabbix_host_name": z_host,
                    "zabbix_ip": ip_any,
                    "zabbix_dns": dns_any,
                }
            )
            continue

        services = sorted(set(c["service"] for c in candidates))

        chosen = None
        match_field = None

        if len(services) == 1:
            chosen = candidates[0]
            match_field = "ip"
        else:
            chosen, match_field, reason = resolve_by_hostname(candidates, z_dns)
            if not chosen:
                conflicts.append(
                    {
                        "type": "ambiguous_ip",
                        "zabbix_host_name": z_host,
                        "zabbix_hostid": hid,
                        "zabbix_ips": z_ips,
                        "services": services,
                        "reason": reason,
                    }
                )
                continue

        if (
            chosen.get("is_old")
            and chosen.get("excel_ip")
            and chosen["excel_ip"] not in warned_old_ips
        ):
            log.warning(
                "Имеет old (единственная запись по IP): %s", chosen.get("excel_ip_raw")
            )
            warned_old_ips.add(chosen["excel_ip"])

        z_ip_out = z_ips[0] if z_ips else chosen.get("excel_ip")

        matched.append(
            {
                "service": chosen["service"],
                "match_field": match_field,
                "excel_host": chosen.get("excel_host"),
                "excel_ip": chosen.get("excel_ip_raw"),
                "zabbix_host_name": z_host,
                "zabbix_ip": z_ip_out,
            }
        )

    return matched, unmatched_zabbix, conflicts


def export_excel(matched, unmatched):
    with pd.ExcelWriter("match_results.xlsx", engine="openpyxl") as writer:
        pd.DataFrame(matched).to_excel(writer, sheet_name="Matched", index=False)
        pd.DataFrame(unmatched).to_excel(
            writer, sheet_name="Unmatched_Zabbix", index=False
        )
    log.info("Результаты сохранены в match_results.xlsx")


def main():
    load_dotenv()
    url = os.getenv("ZABBIX_URL")
    token = os.getenv("ZABBIX_TOKEN")

    if not url or not token:
        log.error("Не заданы ZABBIX_URL / ZABBIX_TOKEN")
        return

    excel_rows = load_excel_rows(r"tags\service_db.xlsx")

    try:
        api = ZabbixAPI(url=url)
        api.login(token=token)
        zabbix_hosts_all = api.host.get(
            output=["hostid", "host", "name", "status"],
            selectInterfaces=["ip", "dns"],
        )
        zabbix_hosts = [h for h in zabbix_hosts_all if str(h.get("status")) == "0"]
        log.info(
            "Получено хостов из Zabbix: %d (активных=%d, отключённых=%d)",
            len(zabbix_hosts_all),
            len(zabbix_hosts),
            len(zabbix_hosts_all) - len(zabbix_hosts),
        )
    except Exception:
        log.exception("Ошибка работы с Zabbix API")
        return

    matched, unmatched, conflicts = assign_services_to_zabbix(zabbix_hosts, excel_rows)

    log.info(
        "ИТОГО: сопоставлено=%d нераспределённых_в_zabbix=%d конфликтов=%d",
        len(matched),
        len(unmatched),
        len(conflicts),
    )

    if conflicts:
        log.error("КОНФЛИКТЫ (первые 10):")
        for c in conflicts[:10]:
            log.error(c)

    export_excel(matched, unmatched)


if __name__ == "__main__":
    main()
