import os
import logging
import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("zbx_match")


def norm(s):
    if not s:
        return None
    s = str(s).strip().lower()
    return s if s else None


def host_variants(host):
    h = norm(host)
    if not h:
        return []
    res = [h]
    if "." in h:
        res.append(h.split(".", 1)[0])
    return list(dict.fromkeys(res))


def load_excel(path):
    df = pd.read_excel(path, engine="openpyxl", dtype=str)
    rows = []

    for service, host, ip in zip(df.iloc[:, 0], df.iloc[:, 13], df.iloc[:, 21]):
        if not service or str(service).lower() == "nan":
            continue

        rows.append({
            "service": str(service).strip(),
            "excel_host": None if not host or str(host).lower() == "nan" else str(host).strip(),
            "excel_ip": None if not ip or str(ip).lower() == "nan" else str(ip).strip(),
            "is_old": bool(ip and "old" in str(ip).lower()),
        })

    log.info("Загружено строк из Excel: %d", len(rows))
    return rows


def load_enabled_zabbix_hosts(api):
    hosts = api.host.get(
        output=["hostid", "name", "status"],
        selectInterfaces=["ip", "dns"],
    )
    enabled = [h for h in hosts if str(h.get("status")) == "0"]

    log.info(
        "Zabbix: всего=%d активных=%d отключённых=%d",
        len(hosts), len(enabled), len(hosts) - len(enabled),
    )
    return enabled


def zabbix_identity_keys(z):
    keys = []

    for iface in z.get("interfaces", []) or []:
        if iface.get("dns"):
            keys.extend(host_variants(iface["dns"]))

    if z.get("name"):
        keys.extend(host_variants(z["name"]))

    return list(dict.fromkeys(keys))


def zabbix_ips(z):
    ips = []
    for iface in z.get("interfaces", []) or []:
        if iface.get("ip"):
            ips.append(iface["ip"])
    return list(dict.fromkeys(ips))



def build_enabled_hostname_set(enabled_hosts):
    s = set()
    for z in enabled_hosts:
        s.update(zabbix_identity_keys(z))
    return s


def build_excel_index(excel_rows, enabled_hostname_keys):
    idx = {}
    dropped = 0

    for r in excel_rows:
        if not r["excel_host"]:
            continue

        variants = host_variants(r["excel_host"])
        if not any(v in enabled_hostname_keys for v in variants):
            dropped += 1
            continue

        for v in variants:
            idx.setdefault(v, []).append(r)

    if dropped:
        log.info("Excel строк отброшено (host не найден среди ENABLED Zabbix): %d", dropped)

    return idx


# ---------- Матч ----------

def assign(enabled_hosts, excel_index):
    matched = []
    unmatched = []
    conflicts = []

    for z in enabled_hosts:
        z_name = z.get("name")
        z_keys = zabbix_identity_keys(z)
        z_ip_list = zabbix_ips(z)

        candidates = []
        for k in z_keys:
            candidates.extend(excel_index.get(k, []))

        if not candidates:
            unmatched.append({
                "zabbix_name": z_name,
                "zabbix_dns": z_keys[0] if z_keys else None,
                "zabbix_ip": z_ip_list[0] if z_ip_list else None,
                "Статус": "Активен",
            })
            continue

        services = sorted(set(c["service"] for c in candidates))
        if len(services) > 1:
            conflicts.append({
                "type": "hostname_conflict",
                "zabbix_name": z_name,
                "services": services,
            })
            continue

        chosen = candidates[0]

        match_field = "hostname"
        if chosen["excel_ip"] and chosen["excel_ip"] in z_ip_list:
            match_field = "hostname+ip"

        if chosen["is_old"]:
            log.warning("Используется запись с old: %s", chosen["excel_ip"])

        matched.append({
            "service": chosen["service"],
            "match_field": match_field,
            "excel_host": chosen["excel_host"],
            "excel_ip": chosen["excel_ip"],
            "zabbix_name": z_name,
            "zabbix_dns": z_keys[0] if z_keys else None,
            "zabbix_ip": z_ip_list[0] if z_ip_list else None,
            "Статус": "Активен",
        })

    return matched, unmatched, conflicts


# ---------- Export ----------

def export_excel(matched, unmatched):
    with pd.ExcelWriter("match_results.xlsx", engine="openpyxl") as writer:
        pd.DataFrame(matched).to_excel(writer, sheet_name="Matched", index=False)
        pd.DataFrame(unmatched).to_excel(writer, sheet_name="Unmatched_Zabbix", index=False)

    log.info("Результаты сохранены в match_results.xlsx")


# ---------- Main ----------

def main():
    load_dotenv()
    url = os.getenv("ZABBIX_URL")
    token = os.getenv("ZABBIX_TOKEN")

    if not url or not token:
        log.error("Не заданы ZABBIX_URL / ZABBIX_TOKEN")
        return

    excel_rows = load_excel(r"tags\service_db.xlsx")

    api = ZabbixAPI(url=url)
    api.login(token=token)

    enabled_hosts = load_enabled_zabbix_hosts(api)
    enabled_hostname_keys = build_enabled_hostname_set(enabled_hosts)

    excel_index = build_excel_index(excel_rows, enabled_hostname_keys)

    matched, unmatched, conflicts = assign(enabled_hosts, excel_index)

    log.info(
        "ИТОГО: сопоставлено=%d нераспределённых=%d конфликтов=%d",
        len(matched), len(unmatched), len(conflicts),
    )

    if conflicts:
        log.error("КОНФЛИКТЫ (первые 10):")
        for c in conflicts[:10]:
            log.error(c)

    export_excel(matched, unmatched)


if __name__ == "__main__":
    main()
