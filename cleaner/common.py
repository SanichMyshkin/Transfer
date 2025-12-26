import os
import logging
import yaml
from datetime import timedelta
from logging.handlers import TimedRotatingFileHandler

# ===== ЛОГИРОВАНИЕ (как в монолите, с ротацией) =====
log_filename = os.path.join(os.path.dirname(__file__), "logs", "cleaner.log")
os.makedirs(os.path.dirname(log_filename), exist_ok=True)

file_handler = TimedRotatingFileHandler(
    log_filename, when="midnight", interval=1, backupCount=7, encoding="utf-8"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[file_handler, logging.StreamHandler()],
)


# ===== ОБЩИЕ УТИЛИТЫ =====
def load_config(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"[LOAD] ❌ Ошибка загрузки конфига '{path}': {e}")
        return None


def get_matching_rule(
    version,
    regex_rules,
    no_match_retention,
    no_match_reserved,
    no_match_min_days_since_last_download,
):
    import re

    #version_lower = version #version.lower()
    matched_rules = []

    for pattern, rules in regex_rules.items():
        if re.match(pattern, version):
            matched_rules.append((pattern, rules))

    if matched_rules:
        best_match = max(matched_rules, key=lambda x: len(x[0]))
        pattern, rules = best_match
        retention_days = rules.get("retention_days")
        reserved = rules.get("reserved")
        min_days_since_last_download = rules.get("min_days_since_last_download")
        retention = (
            timedelta(days=retention_days) if retention_days is not None else None
        )
        return pattern, retention, reserved, min_days_since_last_download

    # === NO-MATCH поведение ===
    if (
        no_match_retention is None
        and no_match_reserved is None
        and no_match_min_days_since_last_download is None
    ):
        # по умолчанию — защищаем от удаления
        return "no-match", None, float("inf"), None

    retention = (
        timedelta(days=no_match_retention) if no_match_retention is not None else None
    )
    return (
        "no-match",
        retention,
        no_match_reserved,
        no_match_min_days_since_last_download,
    )
