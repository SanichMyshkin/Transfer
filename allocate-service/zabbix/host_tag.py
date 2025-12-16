import os
import logging
import pandas as pd
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

"""
Скрипт назначает каждому АКТИВНОМУ хосту Zabbix (status == "0") название команды/сервиса из Excel.

Источник Excel:
- Колонка A  (index 0)  -> service (название команды/сервиса)
- Колонка N  (index 13) -> excel_host (имя хоста)
- Колонка V  (index 21) -> excel_ip (IP; может содержать пометки вроде *_old, -old, old и т.п.)

Источник Zabbix:
- host.get(output=["hostid","host","name","status"], selectInterfaces=["ip","dns"])
- В работу берём ТОЛЬКО хосты со статусом "Активен" (status == "0").
  Хосты Disabled (status == "1") полностью исключаются до любого сопоставления.

Обработка Excel IP с "old":
- Если в excel_ip_raw встречается подстрока "old" (в любом регистре) -> строка помечается как is_old=True.
- Для индексации извлекается "чистый" IP: из строки удаляется "old", затем '-' и '_' заменяются на пробелы,
  берётся первый токен. Если после очистки токенов нет -> excel_ip=None.
- Далее действует правило подавления дублей old:
  если для одного и того же чистого IP в Excel существует запись без "old",
  то все записи с "old" для этого IP исключаются из сопоставления.
  Если запись с "old" для этого IP единственная — она остаётся, но при фактическом назначении
  (когда по ней назначен сервис активному Zabbix-хосту) выводится WARNING "имеет old".

Нормализация hostname:
- Сравнение по hostname выполняется в lower-case.
- Используются варианты имени: полное (FQDN) и short-форма до первой точки (если точка есть).
- Для Zabbix в качестве ключей hostname используются:
  - host
  - name
  - interfaces[].dns
  Все ключи приводятся к набору вариантов (full и short), затем сравниваются с ключами Excel.

Алгоритм сопоставления (для каждого активного Zabbix-хоста):
1) Совместный строгий матч hostname+ip (самый приоритетный):
   - перебираем пары (zabbix_hostname_key, zabbix_ip) и ищем строки Excel с таким же (excel_host_key, excel_ip).
   - Если найдено:
     - если все кандидаты дают один service -> MATCH (match_field="hostname+ip")
     - если кандидаты дают несколько разных service -> CONFLICT (type="ambiguous_host_ip")

2) Если шаг 1 не дал результата — матч только по hostname:
   - ищем строки Excel по совпадающему excel_host_key (без учёта IP).
   - Если найдено:
     - если service один -> MATCH (match_field="hostname")
     - если service несколько -> CONFLICT (type="ambiguous_hostname")
   Важно: hostname тоже может конфликтовать — это ожидаемо и явно учитывается.

3) Если шаги 1-2 не дали результата — матч только по IP:
   - ищем строки Excel по совпадающему excel_ip.
   - Если найдено:
     - если service один -> MATCH (match_field="ip")
     - если service несколько -> CONFLICT (type="ambiguous_ip")
   Примечание: "по IP матчим только если IP даёт один service" — иначе это конфликт.

4) Если не найдено ни по одному правилу — Zabbix-хост попадает в Unmatched_Zabbix.

Результат:
- match_results.xlsx
  - Лист "Matched": 1 строка = 1 активный Zabbix-хост с назначенным service.
  - Лист "Unmatched_Zabbix": активные Zabbix-хосты, которым service не назначен.
- Конфликты выводятся в лог (первые 10), но в Excel отдельным листом не пишутся (можно добавить при необходимости).

Гарантии:
- Disabled-хосты Zabbix не участвуют ни в каких сопоставлениях.
- В Matched не бывает дублей по Zabbix-хостам: один Zabbix-host -> максимум одна строка результата.
"""

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
    host_keys = []

    for iface in z.get("interfaces", []) or []:
        ip = (iface.get("ip") or "").strip()
        d = (iface.get("dns") or "").strip()
        if ip:
            ips.append(ip)
        if d:
            host_keys.extend(host_variants(d))

    host_keys.extend(host_variants(z.get("host") or ""))
    host_keys.extend(host_variants(z.get("name") or ""))

    ips = list(dict.fromkeys([x for x in ips if x]))
    host_keys = list(dict.fromkeys([x for x in host_keys if x]))

    return ips, host_keys


def choose_single_service(candidates):
    services = sorted(set(c["service"] for c in candidates))
    if len(services) == 1:
        return candidates[0], services
    return None, services


def assign_services(zabbix_hosts_enabled, excel_rows):
    excel_rows = filter_old_rows(excel_rows)
    host_ip_index, host_index, ip_index = build_excel_indexes(excel_rows)

    matched = []
    unmatched = []
    conflicts = []

    warned_old_ips = set()

    for z in zabbix_hosts_enabled:
        z_host = z.get("host")
        z_hostid = str(z.get("hostid"))
        z_ips, z_host_keys = get_zabbix_keys(z)

        chosen = None
        match_field = None

        candidates = []
        for hk in z_host_keys:
            for ip in z_ips:
                candidates.extend(host_ip_index.get((hk, ip), []))

        if candidates:
            c, services = choose_single_service(candidates)
            if c:
                chosen = c
                match_field = "hostname+ip"
            else:
                conflicts.append(
                    {
                        "type": "ambiguous_host_ip",
                        "zabbix_host_name": z_host,
                        "zabbix_hostid": z_hostid,
                        "zabbix_ips": z_ips,
                        "zabbix_host_keys": z_host_keys[:5],
                        "services": services,
                    }
                )
                continue

        if not chosen:
            candidates = []
            for hk in z_host_keys:
                candidates.extend(host_index.get(hk, []))

            if candidates:
                c, services = choose_single_service(candidates)
                if c:
                    chosen = c
                    match_field = "hostname"
                else:
                    conflicts.append(
                        {
                            "type": "ambiguous_hostname",
                            "zabbix_host_name": z_host,
                            "zabbix_hostid": z_hostid,
                            "zabbix_host_keys": z_host_keys[:5],
                            "services": services,
                        }
                    )
                    continue

        if not chosen:
            candidates = []
            for ip in z_ips:
                candidates.extend(ip_index.get(ip, []))

            if candidates:
                c, services = choose_single_service(candidates)
                if c:
                    chosen = c
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
            hk_any = z_host_keys[0] if z_host_keys else None
            unmatched.append(
                {
                    "zabbix_host_name": z_host,
                    "zabbix_ip": ip_any,
                    "zabbix_dns": hk_any,
                    "Статус": "Активен",
                }
            )
            continue

        if (
            chosen["is_old"]
            and chosen["excel_ip"]
            and chosen["excel_ip"] not in warned_old_ips
        ):
            log.warning(
                "IP имеет old и является единственным: %s", chosen["excel_ip_raw"]
            )
            warned_old_ips.add(chosen["excel_ip"])

        z_ip_out = z_ips[0] if z_ips else chosen["excel_ip"]

        matched.append(
            {
                "service": chosen["service"],
                "match_field": match_field,
                "excel_host": chosen["excel_host"],
                "excel_ip": chosen["excel_ip_raw"],
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

    matched, unmatched, conflicts = assign_services(enabled_hosts, excel_rows)

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
