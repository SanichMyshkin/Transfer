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
            cleaned = (
                ip_raw.lower()
                .replace("old", "")
                .replace("-", " ")
                .replace("_", " ")
                .strip()
            )
            parts = cleaned.split()
            ip_clean = parts[0] if parts else None

        rows.append({
            "service": service,
            "excel_host": excel_host,
            "excel_ip_raw": ip_raw,
            "excel_ip": ip_clean,
            "is_old": is_old,
        })

    log.info("Загружено строк из Excel: %d", len(rows))
    return rows


def filter_old_rows(rows):
    non_old_ips = set()
    for r in rows:
        if r["excel_ip"] and not r["is_old"]:
            non_old_ips.add(r["excel_ip"])

    out = []
    skipped = 0
    kept = 0

    for r in rows:
        if r["is_old"] and r["excel_ip"] and r["excel_ip"] in non_old_ips:
            skipped += 1
            continue
        if r["is_old"]:
            kept += 1
        out.append(r)

    if skipped:
        log.info("Строк с old исключено из-за дублей по IP: %d", skipped)
    if kept:
        log.warning("Строк с old оставлено (единственные по IP): %d", kept)

    return out


def build_excel_ip_index(rows):
    ip_index = {}
    for r in rows:
        ip = r["excel_ip"]
        if ip:
            ip_index.setdefault(ip, []).append(r)
    return ip_index


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
    dns = set(dict.fromkeys([x for x in dns if x]))

    return ips, dns


def resolve_by_hostname(candidates, z_dns):
    filtered = []

    for c in candidates:
        eh = c.get("excel_host")
        if not eh:
            continue
        ehl = eh.lower()
        if ehl in z_dns or (("." in ehl) and ehl.split(".", 1)[0] in z_dns):
            filtered.append(c)

    services = sorted(set(c["service"] for c in candidates))

    if len(filtered) == 1:
        return filtered[0], "ip+hostname", None

    if len(filtered) > 1:
        services2 = sorted(set(c["service"] for c in filtered))
        return None, None, f"по hostname подходит несколько сервисов {services2}"

    return None, None, f"по IP несколько сервисов {services}, hostname не уточнил"


def assign_services(zabbix_hosts, excel_rows):
    excel_rows = filter_old_rows(excel_rows)
    ip_index = build_excel_ip_index(excel_rows)

    matched = []
    unmatched = []
    conflicts = []

    warned_old_ips = set()

    for z in zabbix_hosts:
        z_host = z.get("host")
        z_hostid = str(z.get("hostid"))

        z_ips, z_dns = get_zabbix_keys(z)

        candidates = []
        for ip in z_ips:
            if ip in ip_index:
                candidates.extend(ip_index[ip])

        if not candidates:
            ip_any = z_ips[0] if z_ips else None
            dns_any = next(iter(z_dns), None)
            unmatched.append({
                "zabbix_host_name": z_host,
                "zabbix_ip": ip_any,
                "zabbix_dns": dns_any,
            })
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
                conflicts.append({
                    "type": "ambiguous_ip",
                    "zabbix_host_name": z_host,
                    "zabbix_hostid": z_hostid,
                    "zabbix_ips": z_ips,
                    "services": services,
                    "reason": reason,
                })
                continue

        if chosen["is_old"] and chosen["excel_ip"] and chosen["excel_ip"] not in warned_old_ips:
            log.warning("IP имеет old и является единственным: %s", chosen["excel_ip_raw"])
            warned_old_ips.add(chosen["excel_ip"])

        z_ip_out = z_ips[0] if z_ips else chosen["excel_ip"]

        matched.append({
            "service": chosen["service"],
            "match_field": match_field,
            "excel_host": chosen["excel_host"],
            "excel_ip": chosen["excel_ip_raw"],
            "zabbix_host_name": z_host,
            "zabbix_ip": z_ip_out,
        })

    return matched, unmatched, conflicts


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

    excel_rows = load_excel_rows(r"tags\service_db.xlsx")

    try:
        api = ZabbixAPI(url=url)
        api.login(token=token)
        all_hosts = api.host.get(
            output=["hostid", "host", "name", "status"],
            selectInterfaces=["ip", "dns"],
        )
        zabbix_hosts = [h for h in all_hosts if str(h.get("status")) == "0"]
        log.info(
            "Получено хостов из Zabbix: %d (активных=%d, отключённых=%d)",
            len(all_hosts), len(zabbix_hosts), len(all_hosts) - len(zabbix_hosts)
        )
    except Exception:
        log.exception("Ошибка работы с Zabbix API")
        return

    matched, unmatched, conflicts = assign_services(zabbix_hosts, excel_rows)

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
