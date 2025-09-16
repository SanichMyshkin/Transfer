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

    for component in components:
        name = component.get("group", "") + ":" + component.get("name", "")
        version = component.get("version", "")
        assets = component.get("assets", [])

        if not assets or not version or not name:
            logging.info(f" ⏭ Пропуск Maven-компонента без имени/версии: {component}")
            continue

        last_modified_strs = [a.get("lastModified") for a in assets if a.get("lastModified")]
        last_download_strs = [a.get("lastDownloaded") for a in assets if a.get("lastDownloaded")]

        if not last_modified_strs:
            logging.info(f" ⏭ Пропуск: нет lastModified у {name}:{version}")
            continue

        try:
            last_modified = max(parse(s) for s in last_modified_strs)
        except Exception:
            logging.info(f" ⏭ Ошибка парсинга lastModified у {name}:{version}")
            continue

        last_download = None
        if last_download_strs:
            try:
                last_download = max(parse(s) for s in last_download_strs)
            except Exception:
                logging.info(f" ⚠ Ошибка парсинга lastDownloaded у {name}:{version}")

        maven_type = detect_maven_type(component)

        rules_cfg = maven_rules.get(maven_type, {}).get("regex_rules", {})
        no_match_retention = maven_rules.get(maven_type, {}).get("no_match_retention_days")
        no_match_reserved = maven_rules.get(maven_type, {}).get("no_match_reserved")
        no_match_min_days_since_last_download = maven_rules.get(maven_type, {}).get(
            "no_match_min_days_since_last_download"
        )

        pattern, retention, reserved, min_days_since_last_download = get_matching_rule(
            version,
            rules_cfg,
            no_match_retention,
            no_match_reserved,
            no_match_min_days_since_last_download,
        )

        component.update({
            "last_modified": last_modified,
            "last_download": last_download,
            "retention": retention,
            "reserved": reserved,
            "pattern": pattern,
            "maven_type": maven_type,
            "min_days_since_last_download": min_days_since_last_download,
        })

        if pattern == "no-match":
            no_match_list.append(component)
        else:
            grouped[(name, pattern, maven_type)].append(component)

    to_delete = []

    # ===== Обработка всех no-match глобально =====
    if no_match_list:
        sorted_no_match = sorted(no_match_list, key=lambda x: x["last_modified"], reverse=True)
        reserved_count = no_match_reserved or 0

        for i, comp in enumerate(sorted_no_match):
            full_name = f"{comp.get('group','')}:{comp.get('name','')}:{comp.get('version','Без версии')}"
            age = now_utc - comp["last_modified"]
            last_download = comp.get("last_download")
            retention = comp.get("retention")
            min_days = comp.get("min_days_since_last_download")

            if i < reserved_count:
                logging.info(f" 📦 Зарезервирован (Maven {comp['maven_type']}): {full_name} | правило (no-match) (позиция {i + 1}/{reserved_count})")
                continue

            if retention is not None and age.days <= retention.days:
                logging.info(f" 📦 Сохранён (Maven {comp['maven_type']}): {full_name} | правило (no-match) (retention: {age.days} дн. ≤ {retention.days})")
                continue

            if last_download and min_days is not None:
                days_since_download = (now_utc - last_download).days
                if days_since_download <= min_days:
                    logging.info(f" 📦 Сохранён (Maven {comp['maven_type']}): {full_name} | правило (no-match) (скачивали {days_since_download} дн. назад ≤ {min_days})")
                    continue

            reason = []
            if retention is not None:
                reason.append(f"retention: {age.days} дн. > {retention.days}")
            if last_download:
                reason.append(f"скачивали {(now_utc - last_download).days} дн. назад")
            else:
                reason.append("скачивали никогда")
            reason_text = ", ".join(reason)

            logging.info(f" 🗑 Удаление (Maven {comp['maven_type']}): {full_name} | правило (no-match) ({reason_text})")
            to_delete.append(comp)

    # ===== Обработка остальных по группам =====
    for (name, pattern, maven_type), group in grouped.items():
        sorted_group = sorted(group, key=lambda x: x["last_modified"], reverse=True)
        for i, comp in enumerate(sorted_group):
            full_name = f"{name}:{comp.get('version','Без версии')}"
            age = now_utc - comp["last_modified"]
            last_download = comp.get("last_download")
            retention = comp.get("retention")
            reserved = comp.get("reserved")
            min_days = comp.get("min_days_since_last_download")

            if reserved is not None and i < reserved:
                logging.info(f" 📦 Зарезервирован (Maven {maven_type}): {full_name} | правило ({pattern}) (позиция {i + 1}/{reserved})")
                continue

            if retention is not None and age.days <= retention.days:
                logging.info(f" 📦 Сохранён (Maven {maven_type}): {full_name} | правило ({pattern}) (retention: {age.days} дн. ≤ {retention.days})")
                continue

            if last_download and min_days is not None:
                days_since_download = (now_utc - last_download).days
                if days_since_download <= min_days:
                    logging.info(f" 📦 Сохранён (Maven {maven_type}): {full_name} | правило ({pattern}) (скачивали {days_since_download} дн. назад ≤ {min_days})")
                    continue

            reason = []
            if retention is not None:
                reason.append(f"retention: {age.days} дн. > {retention.days}")
            if last_download:
                reason.append(f"скачивали {(now_utc - last_download).days} дн. назад")
            else:
                reason.append("скачивали никогда")
            reason_text = ", ".join(reason)

            logging.info(f" 🗑 Удаление (Maven {maven_type}): {full_name} | правило ({pattern}) ({reason_text})")
            to_delete.append(comp)

    logging.info(f" 🧹 Обнаружено к удалению (Maven): {len(to_delete)} компонент(ов)")
    return to_delete

