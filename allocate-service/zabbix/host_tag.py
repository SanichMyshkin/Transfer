import os
import re
import logging
import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("zbx_match")

IP_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")


def is_ip(s):
    s = (s or "").strip()
    if not s or not IP_RE.match(s):
        return False
    parts = s.split(".")
    try:
        return all(0 <= int(p) <= 255 for p in parts)
    except ValueError:
        return False


def norm(s):
    if not s:
        return None
    s = str(s).strip().lower()
    return s if s else None


def variants(host):
    h = norm(host)
    if not h:
        return []
    v = [h]
    if "." in h:
        v.append(h.split(".", 1)[0])
    return list(dict.fromkeys(v))


def load_excel_rows(path):
    df = pd.read_excel(path, engine="openpyxl", dtype=str)
    rows = []

    for service, host, ip in zip(df.iloc[:, 0], df.iloc[:, 13], df.iloc[:, 21]):
        if not service or str(service).lower() == "nan":
            continue

        service = str(service).strip()
        excel_host = (
            None if not host or str(host).lower() == "nan" else str(host).strip()
        )
        ip_raw = None if not ip or str(ip).lower() == "nan" else str(ip).strip()

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

        host_as_ip = is_ip(excel_host) if excel_host else False

        rows.append(
            {
                "service": service,
                "excel_host": excel_host,
                "excel_host_norm": norm(excel_host) if excel_host else None,
                "excel_host_as_ip": host_as_ip,
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


def zabbix_enabled_hostname_set(all_hosts):
    enabled = set()

    for h in all_hosts:
        if str(h.get("status", "0")) != "0":
            continue

        keys = []
        for iface in h.get("interfaces", []) or []:
            dns = (iface.get("dns") or "").strip()
            if dns:
                keys.extend(variants(dns))

        keys.extend(variants(h.get("host") or ""))
        keys.extend(variants(h.get("name") or ""))

        enabled.update([k for k in keys if k])

    return enabled


def hard_filter_excel_rows(rows, enabled_hostnames):
    out = []
    dropped = 0

    for r in rows:
        eh = r.get("excel_host")
        if eh and not r.get("excel_host_as_ip"):
            hs = variants(eh)
            if not any(h in enabled_hostnames for h in hs):
                dropped += 1
                continue
        out.append(r)

    if dropped:
        log.info(
            "Строк Excel исключено (excel_host не найден среди ENABLED Zabbix): %d",
            dropped,
        )

    return out


def build_excel_indexes(rows):
    host_index = {}
    ip_fallback_index = {}

    for r in rows:
        if r.get("excel_host") and not r.get("excel_host_as_ip"):
            for hv in variants(r["excel_host"]):
                host_index.setdefault(hv, []).append(r)

        fallback_ip = None
        if r.get("excel_host_as_ip") and r.get("excel_host"):
            fallback_ip = r["excel_host"].strip()
        elif not r.get("excel_host"):
            fallback_ip = r.get("excel_ip")

        if fallback_ip and is_ip(fallback_ip):
            ip_fallback_index.setdefault(fallback_ip, []).append(r)

    return host_index, ip_fallback_index


def zabbix_keys_and_ips(z):
    ips = []
    keys = []

    for iface in z.get("interfaces", []) or []:
        ip = (iface.get("ip") or "").strip()
        dns = (iface.get("dns") or "").strip()
        if ip:
            ips.append(ip)
        if dns:
            keys.extend(variants(dns))

    keys.extend(variants(z.get("host") or ""))
    keys.extend(variants(z.get("name") or ""))

    ips = list(dict.fromkeys([x for x in ips if x]))
    keys = list(dict.fromkeys([x for x in keys if x]))

    return keys, set(ips)


def pick_single_service(candidates):
    services = sorted(set(c["service"] for c in candidates))
    if len(services) != 1:
        return None, services
    non_old = [c for c in candidates if not c.get("is_old")]
    return (non_old[0] if non_old else candidates[0]), services


def assign_services(enabled_hosts, host_index, ip_fallback_index):
    matched = []
    unmatched = []
    conflicts = []
    warned_old_ips = set()

    for z in enabled_hosts:
        z_host = z.get("host")
        z_hostid = str(z.get("hostid"))
        z_keys, z_ips = zabbix_keys_and_ips(z)

        candidates = []
        for k in z_keys:
            candidates.extend(host_index.get(k, []))

        if candidates:
            chosen, services = pick_single_service(candidates)
            if not chosen:
                conflicts.append(
                    {
                        "type": "ambiguous_hostname",
                        "zabbix_host_name": z_host,
                        "zabbix_hostid": z_hostid,
                        "zabbix_host_keys": z_keys[:5],
                        "services": services,
                    }
                )
                continue

            match_field = "hostname"
            if chosen.get("excel_ip") and chosen["excel_ip"] in z_ips:
                match_field = "hostname+ip"

            if (
                chosen.get("is_old")
                and chosen.get("excel_ip")
                and chosen["excel_ip"] not in warned_old_ips
            ):
                log.warning(
                    "IP имеет old и является единственным: %s",
                    chosen.get("excel_ip_raw"),
                )
                warned_old_ips.add(chosen["excel_ip"])

            z_ip_out = next(iter(z_ips), None)

            matched.append(
                {
                    "service": chosen["service"],
                    "match_field": match_field,
                    "excel_host": chosen.get("excel_host"),
                    "excel_ip": chosen.get("excel_ip_raw"),
                    "zabbix_host_name": z_host,
                    "zabbix_ip": z_ip_out,
                    "Статус": "Активен",
                }
            )
            continue

        ip_candidates = []
        for ip in z_ips:
            ip_candidates.extend(ip_fallback_index.get(ip, []))

        if ip_candidates:
            chosen, services = pick_single_service(ip_candidates)
            if not chosen:
                conflicts.append(
                    {
                        "type": "ambiguous_ip",
                        "zabbix_host_name": z_host,
                        "zabbix_hostid": z_hostid,
                        "zabbix_ips": sorted(list(z_ips)),
                        "services": services,
                    }
                )
                continue

            if (
                chosen.get("is_old")
                and chosen.get("excel_ip")
                and chosen["excel_ip"] not in warned_old_ips
            ):
                log.warning(
                    "IP имеет old и является единственным: %s",
                    chosen.get("excel_ip_raw"),
                )
                warned_old_ips.add(chosen["excel_ip"])

            z_ip_out = next(iter(z_ips), None)

            matched.append(
                {
                    "service": chosen["service"],
                    "match_field": "ip_only",
                    "excel_host": chosen.get("excel_host"),
                    "excel_ip": chosen.get("excel_ip_raw"),
                    "zabbix_host_name": z_host,
                    "zabbix_ip": z_ip_out,
                    "Статус": "Активен",
                }
            )
            continue

        ip_any = next(iter(z_ips), None)
        dns_any = z_keys[0] if z_keys else None
        unmatched.append(
            {
                "zabbix_host_name": z_host,
                "zabbix_ip": ip_any,
                "zabbix_dns": dns_any,
                "Статус": "Активен",
            }
        )

    return matched, unmatched, conflicts


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
    excel_rows = filter_old_rows(excel_rows)

    try:
        api = ZabbixAPI(url=url)
        api.login(token=token)
        all_hosts = api.host.get(
            output=["hostid", "host", "name", "status"],
            selectInterfaces=["ip", "dns"],
        )
        enabled_hosts = [h for h in all_hosts if str(h.get("status")) == "0"]
        log.info(
            "Получено хостов из Zabbix: %d (активных=%d, отключённых=%d)",
            len(all_hosts),
            len(enabled_hosts),
            len(all_hosts) - len(enabled_hosts),
        )
    except Exception:
        log.exception("Ошибка работы с Zabbix API")
        return

    enabled_hostnames = zabbix_enabled_hostname_set(all_hosts)

    excel_rows = hard_filter_excel_rows(excel_rows, enabled_hostnames)

    host_index, ip_fallback_index = build_excel_indexes(excel_rows)

    matched, unmatched, conflicts = assign_services(
        enabled_hosts, host_index, ip_fallback_index
    )

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
