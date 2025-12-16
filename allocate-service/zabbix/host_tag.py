"""
Скрипт назначает каждому АКТИВНОМУ хосту Zabbix (status == "0") название команды/сервиса из Excel.

Главная цель: НЕ допустить ситуации, когда Excel-строка относится к одному хосту (например, отключенному),
а назначение делается другому хосту только из-за общего IP.

Поэтому базовый принцип:
- Сопоставление делается ТОЛЬКО по hostname (обязательно).
- IP используется только как подтверждение (дополнительный сигнал), но НИКОГДА не используется
  для назначения сервиса без совпадения hostname.

Источники данных:
Excel:
- Колонка A  (index 0)  -> service (команда/сервис)
- Колонка N  (index 13) -> excel_host (hostname)
- Колонка V  (index 21) -> excel_ip_raw (может содержать "old" в любом виде)

Zabbix:
- host.get(output=["hostid","host","name","status"], selectInterfaces=["ip","dns"])
- В работу берём ТОЛЬКО активные хосты (status == "0"). Отключённые хосты не обрабатываются.

Нормализация hostname:
- Сравнение выполняется в lower-case.
- Используются варианты имени: полное и short-форма (до первой точки).
- Для Zabbix hostname-ключи: interfaces[].dns, host, name (каждый в full/short вариантах).
- Для Excel hostname-ключи: excel_host (full/short).

Обработка "old" в Excel IP:
- is_old=True если в excel_ip_raw встречается подстрока "old" (любой регистр).
- Для дедупликации извлекается "чистый" IP: удаляем "old", заменяем '-'/'_' на пробелы, берём первый токен;
  если токенов нет -> excel_ip=None.
- Если для одного и того же excel_ip существует запись без old, то все old-записи по этому excel_ip удаляются.
- Если old-запись по excel_ip единственная, она остаётся; WARNING печатается только если по ней реально назначен сервис.

Алгоритм назначения для каждого активного Zabbix-хоста:
1) Находим все Excel-строки, где hostname совпадает с любым из hostname-ключей Zabbix-хоста.
2) Если ничего не найдено -> Unmatched_Zabbix.
3) Если найдено:
   - Если у найденных строк несколько разных service -> CONFLICT (hostname конфликтует).
   - Если service один -> MATCH.
     match_field:
       - "hostname+ip" если у выбранной Excel-строки есть IP и он совпадает с одним из IP Zabbix-хоста.
       - иначе "hostname".
4) IP сам по себе не назначает service и не может подтянуть "чужую" Excel-строку.

Выход:
- match_results.xlsx
  - Лист "Matched": 1 строка = 1 активный Zabbix-хост с назначенным service.
  - Лист "Unmatched_Zabbix": активные Zabbix-хосты без назначения.
- Конфликты выводятся в лог (первые 10).
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


def build_excel_host_index(rows):
    idx = {}
    for r in rows:
        for hv in host_variants(r.get("excel_host")):
            idx.setdefault(hv, []).append(r)
    return idx


def get_zabbix_host_keys_and_ips(z):
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


def pick_prefer_non_old(candidates):
    non_old = [c for c in candidates if not c.get("is_old")]
    return non_old[0] if non_old else candidates[0]


def assign_services(enabled_hosts, excel_rows):
    excel_rows = filter_old_rows(excel_rows)
    host_index = build_excel_host_index(excel_rows)

    matched = []
    unmatched = []
    conflicts = []

    warned_old_ips = set()

    for z in enabled_hosts:
        z_host = z.get("host")
        z_hostid = str(z.get("hostid"))
        z_keys, z_ips = get_zabbix_host_keys_and_ips(z)

        candidates = []
        for k in z_keys:
            candidates.extend(host_index.get(k, []))

        if not candidates:
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
            continue

        services = sorted(set(c["service"] for c in candidates))
        if len(services) > 1:
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

        chosen = pick_prefer_non_old(candidates)

        match_field = "hostname"
        if chosen.get("excel_ip") and chosen["excel_ip"] in z_ips:
            match_field = "hostname+ip"

        if (
            chosen.get("is_old")
            and chosen.get("excel_ip")
            and chosen["excel_ip"] not in warned_old_ips
        ):
            log.warning(
                "IP имеет old и является единственным: %s", chosen.get("excel_ip_raw")
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
