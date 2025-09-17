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


import logging
import re
from datetime import datetime, timezone, timedelta
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

        last_modified_strs = [
            a.get("lastModified") for a in assets if a.get("lastModified")
        ]
        last_download_strs = [
            a.get("lastDownloaded") for a in assets if a.get("lastDownloaded")
        ]
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
        no_match_retention = maven_rules.get(maven_type, {}).get(
            "no_match_retention_days"
        )
        no_match_reserved = maven_rules.get(maven_type, {}).get("no_match_reserved")
        no_match_min_days = maven_rules.get(maven_type, {}).get(
            "no_match_min_days_since_last_download"
        )

        pattern, retention, reserved, min_days = get_matching_rule(
            version, rules_cfg, no_match_retention, no_match_reserved, no_match_min_days
        )

        comp.update(
            {
                "last_modified": last_modified,
                "last_download": last_download,
                "retention": retention,
                "reserved": reserved,
                "pattern": pattern,
                "maven_type": maven_type,
                "min_days_since_last_download": min_days,
            }
        )

        if pattern == "no-match":
            no_match_list.append(comp)
        else:
            grouped[(name, pattern, maven_type)].append(comp)

    saved = []
    to_delete = []

    # ===== Шаг 2: обработка no-match =====
    if no_match_list:
        if (
            no_match_retention is None
            and no_match_reserved is None
            and no_match_min_days is None
        ):
            # Нет правил → сохраняем все
            for comp in no_match_list:
                comp["will_delete"] = False
                comp["delete_reason"] = "нет правил no-match → сохраняем"
                saved.append(comp)
        else:
            sorted_no_match = sorted(
                no_match_list, key=lambda x: x["last_modified"], reverse=True
            )
            reserved_count = no_match_reserved or 0

            for i, comp in enumerate(sorted_no_match):
                age = now_utc - comp["last_modified"]
                last_download = comp.get("last_download")
                min_days = comp.get("min_days_since_last_download")

                if i < reserved_count:
                    comp["will_delete"] = False
                    comp["delete_reason"] = (
                        f"зарезервирован (позиция {i + 1}/{reserved_count}, no-match_reserved)"
                    )
                    saved.append(comp)
                elif no_match_retention is not None and age.days <= no_match_retention:
                    comp["will_delete"] = False
                    comp["delete_reason"] = (
                        f"свежий (возраст {age.days} дн. ≤ {no_match_retention}, no-match_retention_days)"
                    )
                    saved.append(comp)
                elif (
                    last_download
                    and min_days is not None
                    and (now_utc - last_download).days <= min_days
                ):
                    comp["will_delete"] = False
                    comp["delete_reason"] = (
                        f"недавно скачивали ({(now_utc - last_download).days} дн. ≤ {min_days}, no-match_min_days_since_last_download)"
                    )
                    saved.append(comp)
                else:
                    comp["will_delete"] = True
                    if reserved_count and i >= reserved_count:
                        comp["delete_reason"] = (
                            f"удаляется: не попал в reserved ({reserved_count})"
                        )
                    elif (
                        no_match_retention is not None and age.days > no_match_retention
                    ):
                        comp["delete_reason"] = (
                            f"удаляется: возраст {age.days} дн. > {no_match_retention} (no-match_retention_days)"
                        )
                    elif (
                        last_download
                        and min_days is not None
                        and (now_utc - last_download).days > min_days
                    ):
                        comp["delete_reason"] = (
                            f"удаляется: давно не скачивали ({(now_utc - last_download).days} дн. > {min_days}, no-match_min_days_since_last_download)"
                        )
                    else:
                        comp["delete_reason"] = (
                            "удаляется: не соответствует правилам no-match"
                        )
                    to_delete.append(comp)

    # ===== Шаг 3: обработка групп с regex =====
    for (name, pattern, maven_type), group in grouped.items():
        sorted_group = sorted(group, key=lambda x: x["last_modified"], reverse=True)
        reserved = group[0].get("reserved")

        for i, comp in enumerate(sorted_group):
            age = now_utc - comp["last_modified"]
            last_download = comp.get("last_download")
            retention = comp.get("retention")
            min_days = comp.get("min_days_since_last_download")

            if reserved is not None and i < reserved:
                comp["will_delete"] = False
                comp["delete_reason"] = (
                    f"зарезервирован (позиция {i + 1}/{reserved}, reserved)"
                )
                saved.append(comp)
            elif retention is not None and age <= retention:
                comp["will_delete"] = False
                comp["delete_reason"] = (
                    f"свежий (возраст {age.days} дн. ≤ {retention.days}, retention_days)"
                )
                saved.append(comp)
            elif (
                last_download
                and min_days is not None
                and (now_utc - last_download).days <= min_days
            ):
                comp["will_delete"] = False
                comp["delete_reason"] = (
                    f"недавно скачивали ({(now_utc - last_download).days} дн. ≤ {min_days}, min_days_since_last_download)"
                )
                saved.append(comp)
            else:
                comp["will_delete"] = True
                if reserved is not None and i >= reserved:
                    comp["delete_reason"] = (
                        f"удаляется: не попал в reserved ({reserved})"
                    )
                elif retention is not None and age > retention:
                    comp["delete_reason"] = (
                        f"удаляется: возраст {age.days} дн. > {retention.days} (retention_days)"
                    )
                elif (
                    last_download
                    and min_days is not None
                    and (now_utc - last_download).days > min_days
                ):
                    comp["delete_reason"] = (
                        f"удаляется: давно не скачивали ({(now_utc - last_download).days} дн. > {min_days}, min_days_since_last_download)"
                    )
                else:
                    comp["delete_reason"] = "удаляется: не соответствует правилам regex"
                to_delete.append(comp)

    # ===== Шаг 4: Логирование =====
    for comp in saved:
        full_name = f"{comp.get('group', '')}:{comp.get('name', '')}:{comp.get('version', 'Без версии')}"
        logging.info(
            f" ✅ Сохранён (Maven {comp.get('maven_type')}): {full_name} | правило ({comp.get('pattern')}) — причина: {comp.get('delete_reason')}"
        )

    for comp in to_delete:
        full_name = f"{comp.get('group', '')}:{comp.get('name', '')}:{comp.get('version', 'Без версии')}"
        logging.info(
            f" 🗑 Удаление (Maven {comp.get('maven_type')}): {full_name} | правило ({comp.get('pattern')}) — причина: {comp.get('delete_reason')}"
        )

    return to_delete
