import logging
import re
from datetime import datetime, timezone
from collections import defaultdict
from dateutil.parser import parse
from common import get_matching_rule


def detect_maven_type(component):
    """
    Определяет тип Maven-компонента (snapshot или release).
    """
    version = component.get("version", "").lower()

    # 1. Если явно содержит "snapshot" → snapshot
    if "snapshot" in version:
        return "snapshot"

    # 2. Timestamped snapshots (пример: 1.0-20250829.123456-1)
    if re.match(r".*-\d{8}\.\d{6}-\d+", version):
        return "snapshot"

    # 3. Всё остальное → release
    return "release"


def filter_maven_components_to_delete(components, maven_rules):

    now_utc = datetime.now(timezone.utc)
    grouped = defaultdict(list)
    no_match_list = []

    # ===== Шаг 1: собираем компоненты =====
    for comp in components:
        name = comp.get("group", "") + ":" + comp.get("name", "")
        version = comp.get("version", "")
        assets = comp.get("assets", [])

        if not assets or not version or not name:
            continue

        last_modified_strs = [a.get("lastModified") for a in assets if a.get("lastModified")]
        last_download_strs = [a.get("lastDownloaded") for a in assets if a.get("lastDownloaded")]
        if not last_modified_strs:
            continue

        try:
            last_modified = max(parse(s) for s in last_modified_strs)
        except Exception:
            continue

        last_download = None
        if last_download_strs:
            try:
                last_download = max(parse(s) for s in last_download_strs)
            except Exception:
                pass

        maven_type = detect_maven_type(comp)
        rules_cfg = maven_rules.get(maven_type, {}).get("regex_rules", {})
        no_match_retention = maven_rules.get(maven_type, {}).get("no_match_retention_days")
        no_match_reserved = maven_rules.get(maven_type, {}).get("no_match_reserved")
        no_match_min_days = maven_rules.get(maven_type, {}).get("no_match_min_days_since_last_download")

        pattern, retention, reserved, min_days = get_matching_rule(
            version, rules_cfg, no_match_retention, no_match_reserved, no_match_min_days
        )

        comp.update({
            "last_modified": last_modified,
            "last_download": last_download,
            "retention": retention,
            "reserved": reserved,
            "pattern": pattern,
            "maven_type": maven_type,
            "min_days_since_last_download": min_days,
        })

        if pattern == "no-match":
            no_match_list.append(comp)
        else:
            grouped[(name, pattern, maven_type)].append(comp)

    saved = []
    to_delete = []

    # ===== Шаг 2: обработка no-match глобально =====
    if no_match_list:
        if no_match_retention is None and no_match_reserved is None and no_match_min_days is None:
            for comp in no_match_list:
                comp["will_delete"] = False
                saved.append(comp)
        else:
            sorted_no_match = sorted(no_match_list, key=lambda x: x["last_modified"], reverse=True)
            reserved_count = no_match_reserved or 0
            for i, comp in enumerate(sorted_no_match):
                age = now_utc - comp["last_modified"]
                last_download = comp.get("last_download")
                min_days = comp.get("min_days_since_last_download")

                if i < reserved_count:
                    comp["will_delete"] = False
                    saved.append(comp)
                elif no_match_retention is not None and age.days <= no_match_retention:
                    comp["will_delete"] = False
                    saved.append(comp)
                elif last_download and min_days is not None and (now_utc - last_download).days <= min_days:
                    comp["will_delete"] = False
                    saved.append(comp)
                else:
                    comp["will_delete"] = True
                    to_delete.append(comp)

    # ===== Шаг 3: обработка обычных групп =====
    for (name, pattern, maven_type), group in grouped.items():
        sorted_group = sorted(group, key=lambda x: x["last_modified"], reverse=True)
        for i, comp in enumerate(sorted_group):
            age = now_utc - comp["last_modified"]
            last_download = comp.get("last_download")
            retention = comp.get("retention")
            reserved = comp.get("reserved")
            min_days = comp.get("min_days_since_last_download")

            if reserved is not None and i < reserved:
                comp["will_delete"] = False
                saved.append(comp)
            elif retention is not None and age.days <= retention.days:
                comp["will_delete"] = False
                saved.append(comp)
            elif last_download and min_days is not None and (now_utc - last_download).days <= min_days:
                comp["will_delete"] = False
                saved.append(comp)
            else:
                comp["will_delete"] = True
                to_delete.append(comp)

    # ===== Шаг 4: Логирование =====
# ===== Шаг 4: Логирование =====
    for comp in saved:
        full_name = f"{comp.get('group','')}:{comp.get('name','')}:{comp.get('version','Без версии')}"
        pattern = comp.get("pattern")
        reason = []
        if comp.get("reserved") is not None and "will_delete" in comp and not comp["will_delete"]:
            reason.append(f"зарезервирован (позиция {comp.get('position', '?')}/{comp.get('reserved')})")
        if comp.get("retention") is not None:
            reason.append(f"свежий (возраст {(now_utc - comp['last_modified']).days} дн. ≤ {comp['retention'].days})")
        if comp.get("last_download") and comp.get("min_days_since_last_download") is not None:
            reason.append(f"недавно скачивали ({(now_utc - comp['last_download']).days} дн. ≤ {comp['min_days_since_last_download']})")
        if pattern == "no-match":
            reason.append("не попал под условия фильтрации (no-match)")

        logging.info(f" ✅ Сохранён (Maven {comp.get('maven_type')}): {full_name} | правило ({pattern}) — причина: {', '.join(reason)}")

    for comp in to_delete:
        full_name = f"{comp.get('group','')}:{comp.get('name','')}:{comp.get('version','Без версии')}"
        pattern = comp.get("pattern")
        reason = []
        if comp.get("retention") is not None:
            reason.append(f"старый (возраст {(now_utc - comp['last_modified']).days} дн. > {comp['retention'].days})")
        if comp.get("last_download") and comp.get("min_days_since_last_download") is not None:
            reason.append(f"давно не скачивали ({(now_utc - comp['last_download']).days} дн. > {comp['min_days_since_last_download']})")
        if not comp.get("last_download"):
            reason.append("скачивали никогда")

        logging.info(f" 🗑 Удаление (Maven {comp.get('maven_type')}): {full_name} | правило ({pattern}) — причина: {', '.join(reason)}")

    return to_delete
