"""
Скрипт сопоставляет АКТИВНЫЕ (ENABLED) хосты Zabbix с сервисами из Excel.

========================
ИСТОЧНИКИ ДАННЫХ
========================

Excel:
- Колонка A  (index 0)  -> service (название сервиса / команды)
- Колонка N  (index 13) -> excel_host (hostname)
- Колонка V  (index 21) -> excel_ip (IP, может содержать пометку "old")

Zabbix:
- host.get(output=["hostid","name","status"], selectInterfaces=["ip","dns"])
- Используются ТОЛЬКО хосты со статусом status == "0" (ENABLED)

========================
КЛЮЧЕВЫЕ ПРИНЦИПЫ
========================

1. Направление сопоставления
   -------------------------
   Сопоставление всегда выполняется ОТ ZABBIX → К EXCEL.
   Excel-строка НИКОГДА не может быть назначена другому хосту только по IP.

2. Какие поля Zabbix используются
   -------------------------------
   Поле `host` в Zabbix НЕ используется (может быть UUID).
   Для идентификации хоста используются ТОЛЬКО:
   - interfaces[].dns
   - name

3. Предварительная фильтрация Excel
   --------------------------------
   Excel-строка используется ТОЛЬКО если:
   - в ней указан excel_host
   - этот hostname существует среди ENABLED Zabbix-хостов
   Если excel_host найден только у DISABLED-хостов — строка Excel полностью исключается.

4. Матчинг
   --------
   Матч выполняется ТОЛЬКО по hostname (dns / name, full + short).
   IP используется только для информации и пометки match_field, но НЕ для выбора.

5. Обработка old
   --------------
   Строка считается old, если в excel_ip присутствует подстрока "old" (любой регистр).

   Правила:
   - old НЕ влияет на сопоставление, если он единственный вариант → используется + WARNING
   - old НИКОГДА не побеждает non-old
   - old может использоваться для разрешения конфликтов

6. Разрешение конфликтов с old
   ----------------------------
   Если по hostname найдено несколько Excel-строк:

   - Есть non-old и old:
       - Все old отбрасываются
       - Если среди non-old остался 1 сервис → он назначается
       - В Excel помечается, что конфликт был разрешён фильтрацией old

   - После отбрасывания old:
       - Если осталось несколько сервисов → конфликт
       - Если остался один → назначение

   - Если есть ТОЛЬКО old:
       - 1 сервис → назначение + WARNING
       - >1 сервиса → конфликт

7. Результаты
   -----------
   - Matched: назначенные сервисы
   - Unmatched_Zabbix: активные Zabbix-хосты без сервиса
   - Conflicts: хосты, для которых невозможно однозначное назначение

Все автоматические разрешения конфликтов ЯВНО помечаются в Excel.
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


# ---------- Excel ----------

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

    log.info("Excel строк загружено: %d", len(rows))
    return rows


# ---------- Zabbix ----------

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


# ---------- Индексы ----------

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

        variants = host_variants(r["excel_host"])
        if not any(v in enabled_keys for v in variants):
            dropped += 1
            continue

        for v in variants:
            idx.setdefault(v, []).append(r)

    if dropped:
        log.info("Excel строк отброшено (host только у disabled): %d", dropped)

    return idx


# ---------- Матч ----------

def resolve_candidates(candidates):
    non_old = [c for c in candidates if not c["is_old"]]
    old = [c for c in candidates if c["is_old"]]

    if non_old:
        services = sorted(set(c["service"] for c in non_old))
        if len(services) == 1:
            return non_old[0], "resolved_by_old_filter", "old candidates dropped"
        return None, "conflict_non_old", "multiple non-old services"

    services = sorted(set(c["service"] for c in old))
    if len(services) == 1:
        return old[0], "only_old", "only old candidates"
    return None, "conflict_only_old", "multiple old services"


def assign(enabled_hosts, excel_index):
    matched, unmatched, conflicts = [], [], []

    for z in enabled_hosts:
        z_name = z.get("name")
        z_keys = zabbix_keys(z)
        z_ips = zabbix_ips(z)

        candidates = []
        for k in z_keys:
            candidates.extend(excel_index.get(k, []))

        if not candidates:
            unmatched.append({
                "zabbix_name": z_name,
                "zabbix_dns": z_keys[0] if z_keys else None,
                "zabbix_ip": z_ips[0] if z_ips else None,
                "status": "Активен",
            })
            continue

        chosen, resolution, note = resolve_candidates(candidates)
        if not chosen:
            conflicts.append({
                "zabbix_name": z_name,
                "services": sorted(set(c["service"] for c in candidates)),
                "conflict_type": resolution,
            })
            continue

        match_field = "hostname"
        if chosen["excel_ip"] and chosen["excel_ip"] in z_ips:
            match_field = "hostname+ip"

        if chosen["is_old"]:
            log.warning("Используется old-запись: %s", chosen["excel_ip"])

        matched.append({
            "service": chosen["service"],
            "match_field": match_field,
            "excel_host": chosen["excel_host"],
            "excel_ip": chosen["excel_ip"],
            "zabbix_name": z_name,
            "zabbix_dns": z_keys[0] if z_keys else None,
            "zabbix_ip": z_ips[0] if z_ips else None,
            "conflict_resolution": resolution,
            "conflict_note": note,
            "status": "Активен",
        })

    return matched, unmatched, conflicts


# ---------- Export ----------

def export_excel(matched, unmatched, conflicts):
    with pd.ExcelWriter("match_results.xlsx", engine="openpyxl") as writer:
        pd.DataFrame(matched).to_excel(writer, sheet_name="Matched", index=False)
        pd.DataFrame(unmatched).to_excel(writer, sheet_name="Unmatched_Zabbix", index=False)
        pd.DataFrame(conflicts).to_excel(writer, sheet_name="Conflicts", index=False)

    log.info("Результаты сохранены в match_results.xlsx")


# ---------- Main ----------

def main():
    load_dotenv()
    api = ZabbixAPI(url=os.getenv("ZABBIX_URL"))
    api.login(token=os.getenv("ZABBIX_TOKEN"))

    excel_rows = load_excel(r"tags\service_db.xlsx")
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
