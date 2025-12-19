"""
ФИЛЬТРАЦИЯ И СОПОСТАВЛЕНИЕ (полная логика)

1) Zabbix:
   - Берём ТОЛЬКО активные хосты (status == "0").
   - Для идентификации хоста используем ТОЛЬКО:
       * interfaces[].dns
       * name
     Поле host не используем (может быть UUID/техническое).

   - Для каждого Zabbix-хоста строим набор ключей (hostname keys):
       * full (как есть) в lower
       * short (до первой точки) в lower

2) Excel:
   - Читаем:
       * service     (A)
       * excel_host  (N)
       * excel_ip    (V)
   - is_old = True, если в excel_ip встречается подстрока "old" (любой регистр).

3) Предварительная фильтрация Excel (защита от “переноса” старых строк):
   - Excel-строки индексируются ТОЛЬКО если их excel_host (full/short) встречается среди ENABLED Zabbix keys.
     Это выкидывает строки, относящиеся только к disabled/удалённым хостам и запрещает использовать их
     для сопоставления с другими активными хостами.

4) Сопоставление (Zabbix -> Excel), ТОЛЬКО по hostname:
   - Для каждого enabled Zabbix-хоста собираем всех кандидатов Excel по совпадению hostname keys.
   - IP НЕ используется для выбора/подтягивания кандидатов. IP только для поля match_field.

5) Обработка old и конфликтов:
   - Кандидаты делятся на non-old и old.
   - Если есть non-old:
       * old-кандидаты отбрасываются.
       * если среди non-old сервис один -> назначаем non-old.
           - если old-кандидаты были -> conflict_resolution="resolved_by_old_filter",
             conflict_note="old кандидаты отброшены"
           - если old-кандидатов не было -> conflict_resolution/conflict_note пустые
       * если среди non-old несколько сервисов -> конфликт:
           - если old-кандидатов не было -> conflict_type="hostname_multiple_services"
           - если old-кандидаты были и мы их отбросили -> conflict_type="hostname_multiple_services_after_old_drop"
   - Если non-old нет (только old):
       * если сервис один -> назначаем old, conflict_resolution="only_old",
         conflict_note="только old кандидаты" + WARNING в лог
       * если сервисов несколько -> конфликт: conflict_type="hostname_multiple_services_only_old"

6) match_field:
   - "hostname+ip" если excel_ip совпал с одним из zabbix interface ip
   - иначе "hostname"

Результат:
- Matched: назначенные строки + при необходимости пометки conflict_resolution/conflict_note
- Unmatched_Zabbix: enabled хосты без найденных excel-кандидатов
- Conflicts: хосты, где нельзя однозначно выбрать сервис (подробно: name/dns/ip/hostid + services)
"""

import os
import time
import logging
import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI


DRY_RUN = True  # True = только логи, False = реальные изменения

OWNER_NAME_TAG = "host_owner_name"  # вместо host_owner
OWNER_ID_TAG = "host_owner_id"  # новый тег с цифрами из Excel (B)

UPDATE_DELAY_SEC = 1.0  # задержка между запросами на простановку тегов в секундах
MAX_HOSTS_TO_UPDATE = (
    10  # Кол-во которое будет обработано, если не указывать (None), то пойдут в бой все
)

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


def has_tag(tags, tag):
    for t in tags:
        if t.get("tag") == tag:
            return True
    return False


def add_or_replace_tag(tags, tag, value):
    """Добавляет или заменяет тег (чтобы не плодить дубли)."""
    tags = [t.copy() for t in (tags or [])]
    replaced = False
    for t in tags:
        if t.get("tag") == tag:
            t["value"] = value
            replaced = True
            break
    if not replaced:
        tags.append({"tag": tag, "value": value})
    return tags


def load_excel(path):
    """
    Excel:
      A (0)  -> service
      B (1)  -> host_owner_id (цифры)
      N (13) -> excel_host
      V (21) -> excel_ip
    """
    df = pd.read_excel(path, engine="openpyxl", dtype=str)
    rows = []

    for service, owner_id, host, ip in zip(
        df.iloc[:, 0], df.iloc[:, 1], df.iloc[:, 13], df.iloc[:, 21]
    ):
        if not service or str(service).lower() == "nan":
            continue

        excel_ip = None if not ip or str(ip).lower() == "nan" else str(ip).strip()

        rows.append(
            {
                "service": str(service).strip(),
                "owner_id": None
                if not owner_id or str(owner_id).lower() == "nan"
                else str(owner_id).strip(),
                "excel_host": None
                if not host or str(host).lower() == "nan"
                else str(host).strip(),
                "excel_ip": excel_ip,
                "is_old": bool(excel_ip and "old" in excel_ip.lower()),
            }
        )

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
        len(hosts),
        len(enabled),
        len(hosts) - len(enabled),
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


def zabbix_primary_dns(z):
    for iface in z.get("interfaces", []) or []:
        dns = (iface.get("dns") or "").strip()
        if dns:
            return dns
    return None


def zabbix_ips(z):
    ips = []
    for iface in z.get("interfaces", []) or []:
        ip = (iface.get("ip") or "").strip()
        if ip:
            ips.append(ip)
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

    log.info("Excel строк отброшено (host не найден среди ENABLED Zabbix): %d", dropped)
    return idx


def decide_from_candidates(candidates):
    old = [c for c in candidates if c.get("is_old")]
    non_old = [c for c in candidates if not c.get("is_old")]

    if non_old:
        services = sorted(set(c["service"] for c in non_old))
        if len(services) == 1:
            if old:
                return (
                    non_old[0],
                    "resolved_by_old_filter",
                    "old кандидаты отброшены",
                    None,
                )
            return non_old[0], "", "", None

        conflict_type = (
            "hostname_multiple_services_after_old_drop"
            if old
            else "hostname_multiple_services"
        )
        return (
            None,
            "",
            "",
            {
                "conflict_type": conflict_type,
                "services": services,
            },
        )

    services = sorted(set(c["service"] for c in old))
    if len(services) == 1:
        return old[0], "only_old", "только old кандидаты", None

    return (
        None,
        "",
        "",
        {
            "conflict_type": "hostname_multiple_services_only_old",
            "services": services,
        },
    )


def assign(enabled_hosts, excel_index):
    matched, unmatched, conflicts = [], [], []

    for z in enabled_hosts:
        z_hostid = str(z.get("hostid"))
        z_name = z.get("name")
        z_dns = zabbix_primary_dns(z)
        z_ip_list = zabbix_ips(z)
        z_keys = zabbix_keys(z)

        candidates = []
        for k in z_keys:
            candidates.extend(excel_index.get(k, []))

        if not candidates:
            unmatched.append(
                {
                    "zabbix_hostid": z_hostid,
                    "zabbix_name": z_name,
                    "zabbix_dns": z_dns,
                    "zabbix_ip": z_ip_list[0] if z_ip_list else None,
                    "status": "Активен",
                }
            )
            continue

        chosen, resolution, note, conflict_info = decide_from_candidates(candidates)
        if conflict_info:
            conflicts.append(
                {
                    "zabbix_hostid": z_hostid,
                    "zabbix_name": z_name,
                    "zabbix_dns": z_dns,
                    "zabbix_ip": z_ip_list[0] if z_ip_list else None,
                    "conflict_type": conflict_info["conflict_type"],
                    "services": "; ".join(conflict_info["services"]),
                }
            )
            continue

        match_field = "hostname"
        if chosen["excel_ip"] and chosen["excel_ip"] in z_ip_list:
            match_field = "hostname+ip"

        if chosen.get("is_old"):
            log.warning("Используется old-запись: %s", chosen.get("excel_ip"))

        matched.append(
            {
                "service": chosen["service"],  # пойдёт в host_owner_name
                "owner_id": chosen.get("owner_id"),  # пойдёт в host_owner_id
                "match_field": match_field,
                "zabbix_hostid": z_hostid,
                "zabbix_name": z_name,
                "zabbix_dns": z_dns,
                "zabbix_ip": z_ip_list[0] if z_ip_list else None,
            }
        )

    return matched, unmatched, conflicts


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
        len(matched),
        len(unmatched),
        len(conflicts),
    )

    if conflicts:
        log.error("КОНФЛИКТЫ (первые 10):")
        for c in conflicts[:10]:
            log.error(c)

    zabbix_hosts_with_tags = api.host.get(
        output=["hostid", "name"],
        selectInterfaces=["ip", "dns"],
        selectTags="extend",
    )

    hosts_by_id = {h["hostid"]: h for h in zabbix_hosts_with_tags}

    applied = 0
    skipped_existing = 0
    skipped_no_owner_id = 0
    skipped_missing_host = 0

    log.info(
        "=== ПРОСТАНОВКА %s + %s (DRY_RUN=%s) ===",
        OWNER_NAME_TAG,
        OWNER_ID_TAG,
        DRY_RUN,
    )

    for row in matched:
        if MAX_HOSTS_TO_UPDATE is not None and applied >= MAX_HOSTS_TO_UPDATE:
            log.info("Достигнут лимит обработки: %d", MAX_HOSTS_TO_UPDATE)
            break

        hostid = row["zabbix_hostid"]
        owner_name_value = row["service"]
        owner_id_value = row.get("owner_id")

        host = hosts_by_id.get(hostid)
        if not host:
            skipped_missing_host += 1
            continue

        # ВАЖНО: если нет owner_id — скипаем, но лог БОЛЬШИМИ БУКВАМИ
        if not owner_id_value:
            iface0 = (host.get("interfaces") or [{}])[0]
            log.error(
                "SKIPPED: NO OWNER ID FOR MATCHED HOST | HOST=%s | DNS=%s | IP=%s | WOULD_SET %s=%s",
                host.get("name"),
                iface0.get("dns"),
                iface0.get("ip"),
                OWNER_NAME_TAG,
                owner_name_value,
            )
            skipped_no_owner_id += 1
            continue

        tags = host.get("tags", []) or []

        # Если уже есть хотя бы один из тегов — пропускаем
        if has_tag(tags, OWNER_NAME_TAG) or has_tag(tags, OWNER_ID_TAG):
            skipped_existing += 1
            continue

        new_tags = tags
        new_tags = add_or_replace_tag(new_tags, OWNER_NAME_TAG, owner_name_value)
        new_tags = add_or_replace_tag(new_tags, OWNER_ID_TAG, owner_id_value)

        iface = (host.get("interfaces") or [{}])[0]

        log.info(
            "DRY_RUN=%s | HOST=%s | DNS=%s | IP=%s | ADD TAGS: %s=%s, %s=%s",
            DRY_RUN,
            host.get("name"),
            iface.get("dns"),
            iface.get("ip"),
            OWNER_NAME_TAG,
            owner_name_value,
            OWNER_ID_TAG,
            owner_id_value,
        )

        applied += 1

        if DRY_RUN:
            continue

        api.host.update(hostid=hostid, tags=new_tags)

        if UPDATE_DELAY_SEC:
            time.sleep(UPDATE_DELAY_SEC)

    log.info(
        "%s/%s: добавлено=%d пропущено_существует=%d пропущено_нет_owner_id=%d пропущено_нет_хоста=%d",
        OWNER_NAME_TAG,
        OWNER_ID_TAG,
        applied,
        skipped_existing,
        skipped_no_owner_id,
        skipped_missing_host,
    )


if __name__ == "__main__":
    main()
