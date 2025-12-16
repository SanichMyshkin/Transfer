"""
Скрипт назначает каждому АКТИВНОМУ хосту Zabbix (status == "0") название команды/сервиса из Excel.

Excel (по колонкам):
- A  (index 0)  -> service
- N  (index 13) -> excel_host (может быть hostname или иногда IP/пусто)
- V  (index 21) -> excel_ip_raw (может содержать "old" в любом виде)

Zabbix:
- host.get(output=["hostid","host","name","status"], selectInterfaces=["ip","dns"])
- В работу берём ТОЛЬКО активные хосты: status == "0" (Enabled).
  Disabled хосты (status == "1") никогда не назначаются и используются только для фильтрации Excel.

Ключевое правило (чтобы не “пришить” старый Excel-хост к другому enabled-хосту по общему IP):
1) Если в Excel указан hostname (excel_host) и этот hostname существует в Zabbix ТОЛЬКО как Disabled
   (т.е. есть среди disabled, но нет среди enabled) — такая строка Excel считается устаревшей и полностью
   исключается из сопоставления. Даже если IP совпадает с каким-то enabled-хостом.

Использование IP:
- Если в Excel указан hostname (не IP) — IP НЕ используется для назначения другому хосту.
  IP в этом случае служит только для подтверждения в поле match_field ("hostname+ip") если совпал.
- Матч "только по IP" разрешён ТОЛЬКО для Excel-строк, где hostname отсутствует или является IP.

Обработка "old" в Excel IP:
- is_old=True, если в excel_ip_raw встречается подстрока "old" (любой регистр).
- Для дедупликации извлекается excel_ip:
  удаляем "old", заменяем '-' и '_' на пробелы, берём первый токен; если токенов нет -> None.
- Если для одного excel_ip существует запись без old, то все old-записи по этому excel_ip исключаются.
- Если old-запись единственная по excel_ip — остаётся, и WARNING печатается только когда по ней реально назначен сервис.

Алгоритм назначения (для каждого enabled Zabbix-хоста):
1) Матч по hostname:
   - ищем Excel-строки, у которых excel_host совпадает с любым hostname-ключом Zabbix-хоста (full/short, lower).
   - если найдено несколько разных service -> CONFLICT (ambiguous_hostname)
   - если один service -> MATCH (match_field="hostname" или "hostname+ip" если IP совпал)

2) Fallback по IP (строго ограничен):
   - используется ТОЛЬКО если по hostname ничего не нашли
   - и только по Excel-строкам, у которых excel_host пустой или является IP (т.е. "host-as-ip")
   - если по IP найдено несколько разных service -> CONFLICT (ambiguous_ip)
   - если один service -> MATCH (match_field="ip_only")

Выход:
- match_results.xlsx
  - "Matched": назначенные связи (1 строка = 1 enabled Zabbix-хост)
  - "Unmatched_Zabbix": enabled хосты без назначения
- Конфликты печатаются в лог (первые 10).
"""

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
    if not s:
        return False
    if not IP_RE.match(s):
        return False
    parts = s.split(".")
    try:
        return all(0 <= int(p) <= 255 for p in parts)
    except ValueError:
        return False


def normalize_host(s):
    if not s:
        return None
    s = str(s).strip().lower()
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
                "excel_host_norm": normalize_host(excel_host) if excel_host else None,
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


def zabbix_hostname_sets(all_hosts):
    enabled = set()
    disabled = set()

    for h in all_hosts:
        status = str(h.get("status", "0"))
        target = enabled if status == "0" else disabled

        keys = []
        for iface in h.get("interfaces", []) or []:
            dns = (iface.get("dns") or "").strip()
            if dns:
                keys.extend(host_variants(dns))

        keys.extend(host_variants(h.get("host") or ""))
        keys.extend(host_variants(h.get("name") or ""))

        target.update([k for k in keys if k])

    return enabled, disabled


def filter_excel_disabled_only_host(rows, enabled_hostnames, disabled_hostnames):
    out = []
    dropped = 0

    for r in rows:
        hn = r.get("excel_host_norm")
        if hn and not r.get("excel_host_as_ip"):
            variants = host_variants(hn)
            in_enabled = any(v in enabled_hostnames for v in variants)
            in_disabled = any(v in disabled_hostnames for v in variants)
            if in_disabled and not in_enabled:
                dropped += 1
                continue

        out.append(r)

    if dropped:
        log.info(
            "Строк Excel исключено (hostname есть только в disabled Zabbix): %d",
            dropped,
        )

    return out


def build_excel_indexes(rows):
    host_index = {}
    ip_fallback_index = {}

    for r in rows:
        svc = r["service"]

        if r.get("excel_host_norm") and not r.get("excel_host_as_ip"):
            for hv in host_variants(r["excel_host_norm"]):
                host_index.setdefault(hv, []).append(r)

        fallback_ip = None
        if r.get("excel_host_as_ip") and r.get("excel_host"):
            fallback_ip = r["excel_host"].strip()
        elif r.get("excel_host") is None or (str(r.get("excel_host")).strip() == ""):
            fallback_ip = r.get("excel_ip")

        if fallback_ip and is_ip(fallback_ip):
            ip_fallback_index.setdefault(fallback_ip, []).append(r)

    return host_index, ip_fallback_index


def get_zabbix_keys_and_ips(z):
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

    return keys, set(ips)


def pick_service_single(candidates):
    services = sorted(set(c["service"] for c in candidates))
    if len(services) != 1:
        return None, services
    non_old = [c for c in candidates if not c.get("is_old")]
    return (non_old[0] if non_old else candidates[0]), services


def assign_services(enabled_hosts, excel_rows, host_index, ip_fallback_index):
    matched = []
    unmatched = []
    conflicts = []
    warned_old_ips = set()

    for z in enabled_hosts:
        z_host = z.get("host")
        z_hostid = str(z.get("hostid"))
        z_keys, z_ips = get_zabbix_keys_and_ips(z)

        candidates = []
        for k in z_keys:
            candidates.extend(host_index.get(k, []))

        if candidates:
            chosen, services = pick_service_single(candidates)
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
            chosen, services = pick_service_single(ip_candidates)
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

    enabled_hostnames, disabled_hostnames = zabbix_hostname_sets(all_hosts)

    excel_rows = filter_excel_disabled_only_host(
        excel_rows, enabled_hostnames, disabled_hostnames
    )

    host_index, ip_fallback_index = build_excel_indexes(excel_rows)

    matched, unmatched, conflicts = assign_services(
        enabled_hosts, excel_rows, host_index, ip_fallback_index
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
