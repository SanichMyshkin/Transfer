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


def _retention_days(retention):
    """Преобразует retention в количество дней (int)."""
    if retention is None:
        return None
    if hasattr(retention, "days"):  # timedelta
        return retention.days
    return int(retention)


def filter_maven_components_to_delete(components, maven_rules):
    now_utc = datetime.now(timezone.utc)
    grouped = defaultdict(list)
    grouped_no_match = defaultdict(list)

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
            grouped_no_match[(name, maven_type)].append(comp)
        else:
            grouped[(name, pattern, maven_type)].append(comp)

    saved = []
    to_delete = []

    # ===== Шаг 2: обработка no-match =====
    for (name, maven_type), group in grouped_no_match.items():
        sorted_group = sorted(group, key=lambda x: x["last_modified"], reverse=True)

        if (
            maven_rules.get(maven_type, {}).get("no_match_retention_days") is None
            and maven_rules.get(maven_type, {}).get("no_match_reserved") is None
            and maven_rules.get(maven_type, {}).get(
                "no_match_min_days_since_last_download"
            )
            is None
        ):
            for comp in sorted_group:
                comp["will_delete"] = False
                comp["delete_reason"] = f"нет правил no-match → сохраняем ({name})"
                saved.append(comp)
            continue

        for i, comp in enumerate(sorted_group):
            age_days = (now_utc - comp["last_modified"]).days
            last_download = comp.get("last_download")
            reserved = comp.get("reserved") or 0
            retention = comp.get("retention")
            min_days = comp.get("min_days_since_last_download")

            # 1) reserved
            if reserved and i < reserved:
                comp["will_delete"] = False
                comp["delete_reason"] = (
                    f"зарезервирован (позиция {i + 1}/{reserved}, no-match, {name})"
                )
                saved.append(comp)
                continue

            # 2) retention
            limit = _retention_days(retention)
            if limit is not None and age_days <= limit:
                comp["will_delete"] = False
                comp["delete_reason"] = (
                    f"свежий (возраст {age_days} дн. ≤ {limit} дн., no-match, {name})"
                )
                saved.append(comp)
                continue

            # 3) last download
            if min_days is not None and last_download:
                days_since_dl = (now_utc - last_download).days
                if days_since_dl <= int(min_days):
                    comp["will_delete"] = False
                    comp["delete_reason"] = (
                        f"недавно скачивали ({days_since_dl} дн. ≤ {int(min_days)} дн., no-match, {name})"
                    )
                    saved.append(comp)
                    continue

            # иначе → удаляем
            failures = []
            if reserved:
                failures.append(f"позиция {i + 1} > reserved {reserved}")
            if limit is not None:
                failures.append(f"возраст {age_days} дн. > retention {limit} дн.")
            if min_days is not None:
                if last_download:
                    failures.append(
                        f"последнее скачивание {(now_utc - last_download).days} дн. > min_days {int(min_days)} дн."
                    )
                else:
                    failures.append(
                        f"нет данных о скачивании (требуется min_days={int(min_days)} дн.)"
                    )

            reason = (
                f"удаляется по правилам no-match ({name}): " + "; ".join(failures)
                if failures
                else f"нет условий сохранения (no-match, {name}) → удаляем"
            )
            comp["will_delete"] = True
            comp["delete_reason"] = reason
            to_delete.append(comp)

    # ===== Шаг 3: обработка regex-групп =====
    for (name, pattern, maven_type), group in grouped.items():
        sorted_group = sorted(group, key=lambda x: x["last_modified"], reverse=True)
        reserved = group[0].get("reserved")

        for i, comp in enumerate(sorted_group):
            age_days = (now_utc - comp["last_modified"]).days
            last_download = comp.get("last_download")
            retention = comp.get("retention")
            min_days = comp.get("min_days_since_last_download")

            # 1) reserved
            if reserved is not None and i < reserved:
                comp["will_delete"] = False
                comp["delete_reason"] = (
                    f"зарезервирован (позиция {i + 1}/{reserved}, правило '{pattern}', {name})"
                )
                saved.append(comp)
                continue

            # 2) retention
            limit = _retention_days(retention)
            if limit is not None and age_days <= limit:
                comp["will_delete"] = False
                comp["delete_reason"] = (
                    f"свежий (возраст {age_days} дн. ≤ {limit} дн., правило '{pattern}', {name})"
                )
                saved.append(comp)
                continue

            # 3) last download
            if min_days is not None and last_download:
                days_since_dl = (now_utc - last_download).days
                if days_since_dl <= int(min_days):
                    comp["will_delete"] = False
                    comp["delete_reason"] = (
                        f"недавно скачивали ({days_since_dl} дн. ≤ {int(min_days)} дн., правило '{pattern}', {name})"
                    )
                    saved.append(comp)
                    continue

            # иначе → удаляем
            failures = []
            if reserved is not None:
                failures.append(f"позиция {i + 1} > reserved {reserved}")
            if limit is not None:
                failures.append(f"возраст {age_days} дн. > retention {limit} дн.")
            if min_days is not None:
                if last_download:
                    failures.append(
                        f"последнее скачивание {(now_utc - last_download).days} дн. > min_days {int(min_days)} дн."
                    )
                else:
                    failures.append(
                        f"нет данных о скачивании (требуется min_days={int(min_days)} дн.)"
                    )

            reason = (
                f"удаляется по правилу '{pattern}' ({name}): " + "; ".join(failures)
                if failures
                else f"не соответствует правилу '{pattern}' → удаляем"
            )
            comp["will_delete"] = True
            comp["delete_reason"] = reason
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
