"""
Скрипт назначает каждому АКТИВНОМУ хосту Zabbix (status == "0") название команды/сервиса из Excel.

Excel:
- A (0)  -> service
- N (13) -> excel_host
- V (21) -> excel_ip_raw

Zabbix:
- host.get(output=["hostid","host","name","status"], selectInterfaces=["ip","dns"])
- В работу берём ТОЛЬКО enabled (status=="0").

Excel IP с "old":
- is_old=True если в excel_ip_raw есть подстрока "old" (любой регистр).
- excel_ip для индексов: удаляем "old", заменяем '-'/'_' на пробелы, берём первый токен; если пусто -> None.
- Если для excel_ip есть запись без old, то все old по этому IP отбрасываются.

Доп. фильтрация Excel по статусам Zabbix:
- Строки Excel вырезаются, если их IP или hostname встречаются в Zabbix только у disabled (нет enabled).

Алгоритм матча для каждого enabled Zabbix-хоста:
1) hostname+ip -> один service, prefer non-old
2) hostname    -> один service, prefer non-old
3) ip          -> один service, prefer non-old; иначе конфликт
Unmatched_Zabbix: enabled хосты без назначения.
"""

import os
import logging
import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("zbx_match")


def normalize_host(s):
    if not s:
        return None
    s = s.strip().lower()
    return s if s else None


def host_variants(host):
    h = normalize_host(host)
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


def zabbix_enabled_disabled_sets(all_hosts):
    enabled_ips = set()
    disabled_ips = set()
    enabled_keys = set()
    disabled_keys = set()

    for h in all_hosts:
        status = str(h.get("status", "0"))
        is_enabled = status == "0"

        ips = []
        keys = []
        for iface in h.get("interfaces", []) or []:
            ip = (iface.get("ip") or "").strip()
            dns = (iface.get("dns") or "").strip()
            if ip:
                ips.append(ip)
            if dns:
                keys.extend(host_variants(dns))

        keys.extend(host_variants(h.get("host") or ""))
        keys.extend(host_variants(h.get("name") or ""))

        if is_enabled:
            enabled_ips.update(ips)
            enabled_keys.update(keys)
        else:
            disabled_ips.update(ips)
            disabled_keys.update(keys)

    disabled_only_ips = disabled_ips - enabled_ips
    disabled_only_keys = disabled_keys - enabled_keys
    return enabled_ips, enabled_keys, disabled_only_ips, disabled_only_keys


def filter_excel_by_zabbix_status(rows, disabled_only_ips, disabled_only_keys):
    out = []
    dropped = 0

    for r in rows:
        ip = r.get("excel_ip")
        eh = r.get("excel_host")
        keys = host_variants(eh)

        bad = False
        if ip and ip in disabled_only_ips:
            bad = True
        if not bad and keys and any(k in disabled_only_keys for k in keys):
            bad = True

        if bad:
            dropped += 1
            continue

        out.append(r)

    if dropped:
        log.info(
            "Строк Excel исключено как относящихся только к disabled в Zabbix: %d",
            dropped,
        )

    return out


def build_excel_indexes(rows):
    host_ip_index = {}
    host_index = {}
    ip_index = {}

    for r in rows:
        entry = {
            "service": r["service"],
            "excel_host": r["excel_host"],
            "excel_ip_raw": r["excel_ip_raw"],
            "excel_ip": r["excel_ip"],
            "is_old": r["is_old"],
        }

        ip = entry["excel_ip"]
        eh = entry["excel_host"]

        if ip:
            ip_index.setdefault(ip, []).append(entry)

        for hv in host_variants(eh):
            host_index.setdefault(hv, []).append(entry)
            if ip:
                host_ip_index.setdefault((hv, ip), []).append(entry)

    return host_ip_index, host_index, ip_index


def get_zabbix_keys(z):
    ips = []
    keys = []

    for iface in z.get("interfaces", []) or []:
        ip = (iface.get("ip") or "").strip()
        dns = (iface.get("dns") or "").strip()
        if ip:
            ips.append(ip)
        if dns:
            keys.extend(host_variants(dns))

    keys.extend(host_variants(z.get("host") or ""))
    keys.extend(host_variants(z.get("name") or ""))

    ips = list(dict.fromkeys([x for x in ips if x]))
    keys = list(dict.fromkeys([x for x in keys if x]))
    return ips, keys


def pick_prefer_non_old(candidates):
    services = sorted(set(c["service"] for c in candidates))
    if len(services) != 1:
        return None, services

    non_old = [c for c in candidates if not c.get("is_old")]
    if non_old:
        return non_old[0], services

    return candidates[0], services


def assign_services(enabled_hosts, all_hosts, excel_rows):
    _, _, disabled_only_ips, disabled_only_keys = zabbix_enabled_disabled_sets(
        all_hosts
    )

    excel_rows = filter_old_rows(excel_rows)
    excel_rows = filter_excel_by_zabbix_status(
        excel_rows, disabled_only_ips, disabled_only_keys
    )

    host_ip_index, host_index, ip_index = build_excel_indexes(excel_rows)

    matched = []
    unmatched = []
    conflicts = []
    warned_old_ips = set()

    for z in enabled_hosts:
        z_host = z.get("host")
        z_hostid = str(z.get("hostid"))
        z_ips, z_keys = get_zabbix_keys(z)

        candidates = []
        for hk in z_keys:
            for ip in z_ips:
                candidates.extend(host_ip_index.get((hk, ip), []))

        if candidates:
            chosen, services = pick_prefer_non_old(candidates)
            if chosen:
                match_field = "hostname+ip"
            else:
                conflicts.append(
                    {
                        "type": "ambiguous_host_ip",
                        "zabbix_host_name": z_host,
                        "zabbix_hostid": z_hostid,
                        "zabbix_ips": z_ips,
                        "zabbix_host_keys": z_keys[:5],
                        "services": services,
                    }
                )
                continue
        else:
            chosen = None
            match_field = None

        if not chosen:
            candidates = []
            for hk in z_keys:
                candidates.extend(host_index.get(hk, []))

            if candidates:
                chosen, services = pick_prefer_non_old(candidates)
                if chosen:
                    match_field = "hostname"
                else:
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

        if not chosen:
            candidates = []
            for ip in z_ips:
                candidates.extend(ip_index.get(ip, []))

            if candidates:
                chosen, services = pick_prefer_non_old(candidates)
                if chosen:
                    match_field = "ip"
                else:
                    conflicts.append(
                        {
                            "type": "ambiguous_ip",
                            "zabbix_host_name": z_host,
                            "zabbix_hostid": z_hostid,
                            "zabbix_ips": z_ips,
                            "services": services,
                        }
                    )
                    continue

        if not chosen:
            ip_any = z_ips[0] if z_ips else None
            key_any = z_keys[0] if z_keys else None
            unmatched.append(
                {
                    "zabbix_host_name": z_host,
                    "zabbix_ip": ip_any,
                    "zabbix_dns": key_any,
                    "Статус": "Активен",
                }
            )
            continue

        if (
            chosen.get("is_old")
            and chosen.get("excel_ip")
            and chosen["excel_ip"] not in warned_old_ips
        ):
            log.warning(
                "IP имеет old и является единственным: %s", chosen.get("excel_ip_raw")
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

    matched, unmatched, conflicts = assign_services(
        enabled_hosts, all_hosts, excel_rows
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
