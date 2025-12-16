"""
ФИЛЬТРАЦИЯ И СОПОСТАВЛЕНИЕ (полная логика)

1) Zabbix:
   - Берём ТОЛЬКО активные хосты (status == "0").
   - Для идентификации хоста используем ТОЛЬКО:
       * interfaces[].dns
       * name
     Поле host не используем (часто UUID/техническое).

   - Для каждого Zabbix-хоста строим набор ключей (hostname keys):
       * full (как есть) в lower
       * short (до первой точки) в lower

2) Excel:
   - Читаем:
       * service  (A)
       * excel_host (N)
       * excel_ip   (V)
   - is_old = True, если в excel_ip встречается подстрока "old" (любой регистр).

3) Предварительная фильтрация Excel:
   - Excel-строки индексируем ТОЛЬКО если их excel_host (full/short) встречается среди ENABLED Zabbix keys.
     Это автоматически выкидывает строки, относящиеся только к disabled/удалённым хостам.

4) Сопоставление (Zabbix -> Excel), ТОЛЬКО по hostname:
   - Для каждого enabled Zabbix-хоста собираем всех кандидатов Excel по совпадению ключей hostname.
   - IP НЕ используется для выбора/подтягивания кандидатов. IP только для поля match_field.

5) Обработка old и конфликтов:
   - Кандидаты делятся на non-old и old.
   - Если есть non-old:
       * old-кандидаты отбрасываются.
       * если среди non-old сервис один -> назначаем non-old.
           - если old-кандидаты были -> conflict_resolution="resolved_by_old_filter",
             conflict_note="были old-кандидаты, выбрали non-old"
           - если old-кандидатов не было -> conflict_resolution/conflict_note пустые
       * если среди non-old несколько сервисов -> конфликт (Conflicts),
         conflict_type="conflict_non_old_services"
   - Если non-old нет (только old):
       * если сервис один -> назначаем old, conflict_resolution="only_old",
         conflict_note="только old-кандидаты"
         + WARNING в лог
       * если сервисов несколько -> конфликт (Conflicts),
         conflict_type="conflict_only_old_services"

6) match_field:
   - "hostname+ip" если excel_ip совпал с одним из zabbix interface ip
   - иначе "hostname"

Результат:
- Matched: назначенные строки + при необходимости пометки conflict_resolution/conflict_note
- Unmatched_Zabbix: enabled хосты без найденных excel-кандидатов
- Conflicts: хосты, где нельзя однозначно выбрать сервис
"""

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
    return str(s).strip().lower() or None


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

        excel_ip = None if not ip or str(ip).lower() == "nan" else str(ip).strip()

        rows.append({
            "service": str(service).strip(),
            "excel_host": None if not host or str(host).lower() == "nan" else str(host).strip(),
            "excel_ip": excel_ip,
            "is_old": bool(excel_ip and "old" in excel_ip.lower()),
        })

    log.info("Excel строк загружено: %d", len(rows))
    return rows


def load_enabled_zabbix(api):
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


def zabbix_keys(z):
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
        s.update(zabbix_keys(z))
    return s


def build_excel_index(excel_rows, enabled_keys):
    idx = {}
    dropped = 0

    for r in excel_rows:
        if not r["excel_host"]:
            continue

        vars_ = host_variants(r["excel_host"])
        if not any(v in enabled_keys for v in vars_):
            dropped += 1
            continue

        for v in vars_:
            idx.setdefault(v, []).append(r)

    if dropped:
        log.info("Excel строк отброшено (host не найден среди ENABLED Zabbix): %d", dropped)

    return idx


def decide_from_candidates(candidates):
    old = [c for c in candidates if c.get("is_old")]
    non_old = [c for c in candidates if not c.get("is_old")]

    if non_old:
        services = sorted(set(c["service"] for c in non_old))
        if len(services) == 1:
            chosen = non_old[0]
            if old:
                return chosen, "resolved_by_old_filter", "были old-кандидаты, выбрали non-old", False, None
            return chosen, "", "", False, None
        return None, "", "", True, {
            "conflict_type": "conflict_non_old_services",
            "services": services,
        }

    services = sorted(set(c["service"] for c in old))
    if len(services) == 1:
        return old[0], "only_old", "только old-кандидаты", False, None
    return None, "", "", True, {
        "conflict_type": "conflict_only_old_services",
        "services": services,
    }


def assign(enabled_hosts, excel_index):
    matched, unmatched, conflicts = [], [], []

    for z in enabled_hosts:
        z_name = z.get("name")
        z_keys = zabbix_keys(z)
        z_ip_list = zabbix_ips(z)

        candidates = []
        for k in z_keys:
            candidates.extend(excel_index.get(k, []))

        if not candidates:
            unmatched.append({
                "zabbix_name": z_name,
                "zabbix_dns": z_keys[0] if z_keys else None,
                "zabbix_ip": z_ip_list[0] if z_ip_list else None,
                "status": "Активен",
            })
            continue

        chosen, resolution, note, is_conflict, conflict_info = decide_from_candidates(candidates)
        if is_conflict:
            conflicts.append({
                "zabbix_name": z_name,
                "zabbix_dns": z_keys[0] if z_keys else None,
                "conflict_type": conflict_info["conflict_type"],
                "services": conflict_info["services"],
            })
            continue

        match_field = "hostname"
        if chosen["excel_ip"] and chosen["excel_ip"] in z_ip_list:
            match_field = "hostname+ip"

        if chosen.get("is_old"):
            log.warning("Используется old-запись: %s", chosen.get("excel_ip"))

        matched.append({
            "service": chosen["service"],
            "match_field": match_field,
            "excel_host": chosen["excel_host"],
            "excel_ip": chosen["excel_ip"],
            "zabbix_name": z_name,
            "zabbix_dns": z_keys[0] if z_keys else None,
            "zabbix_ip": z_ip_list[0] if z_ip_list else None,
            "conflict_resolution": resolution,
            "conflict_note": note,
            "status": "Активен",
        })

    return matched, unmatched, conflicts


def export_excel(matched, unmatched, conflicts):
    with pd.ExcelWriter("match_results.xlsx", engine="openpyxl") as writer:
        pd.DataFrame(matched).to_excel(writer, sheet_name="Matched", index=False)
        pd.DataFrame(unmatched).to_excel(writer, sheet_name="Unmatched_Zabbix", index=False)
        pd.DataFrame(conflicts).to_excel(writer, sheet_name="Conflicts", index=False)
    log.info("Результаты сохранены в match_results.xlsx")


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

    enabled_hosts = load_enabled_zabbix(api)
    enabled_keys = build_enabled_hostname_set(enabled_hosts)
    excel_index = build_excel_index(excel_rows, enabled_keys)

    matched, unmatched, conflicts = assign(enabled_hosts, excel_index)

    log.info(
        "ИТОГО: сопоставлено=%d нераспределённых=%d конфликтов=%d",
        len(matched), len(unmatched), len(conflicts),
    )

    export_excel(matched, unmatched, conflicts)


if __name__ == "__main__":
    main()
