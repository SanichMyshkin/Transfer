import os
import logging
import requests
import yaml
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse
from collections import defaultdict
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv
import urllib3
import re

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

USER_NAME = os.getenv("USER_NAME")
PASSWORD = os.getenv("PASSWORD")
BASE_URL = os.getenv("BASE_URL")

log_filename = os.path.join(os.path.dirname(__file__), "logs", "cleaner.log")
os.makedirs(os.path.dirname(log_filename), exist_ok=True)

file_handler = TimedRotatingFileHandler(
    log_filename, when="midnight", interval=1, backupCount=7, encoding="utf-8"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        file_handler,
        logging.StreamHandler(),
    ],
)


def load_config(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"[LOAD] ❌ Ошибка загрузки конфига '{path}': {e}")
        return None


def get_repository_format(repo_name):
    url = f"{BASE_URL}service/rest/v1/repositories"
    try:
        response = requests.get(
            url, auth=(USER_NAME, PASSWORD), timeout=10, verify=False
        )
        response.raise_for_status()
        for repo in response.json():
            if repo.get("name") == repo_name:
                return repo.get("format")
    except Exception as e:
        logging.error(
            f"[FORMAT] ❌ Не удалось определить формат репозитория {repo_name}: {e}"
        )
    return None


def get_repository_items(repo_name, repo_format):
    items = []
    continuation_token = None
    url = f"{BASE_URL}service/rest/v1/"
    url += "assets" if repo_format == "raw" else "components"

    while True:
        params = {"repository": repo_name}
        if continuation_token:
            params["continuationToken"] = continuation_token
        try:
            response = requests.get(
                url, auth=(USER_NAME, PASSWORD), params=params, timeout=10, verify=False
            )
            response.raise_for_status()
            data = response.json()
            items.extend(data.get("items", []))
            continuation_token = data.get("continuationToken")
            if not continuation_token:
                break
        except Exception as e:
            logging.error(f"[API] ❌ Ошибка при получении данных из '{repo_name}': {e}")
            return []
    return items


def convert_raw_assets_to_components(assets):
    components = []
    for asset in assets:
        path = asset.get("path", "")
        if not path or "/" not in path:
            continue
        name = os.path.dirname(path) or "/"  # "/" если файл в корне
        version = os.path.basename(path)
        if not version:
            continue
        components.append(
            {
                "id": asset.get("id"),
                "name": name,
                "version": version,
                "assets": [asset],
            }
        )
    return components


def delete_component(
    component_id, component_name, component_version, dry_run, use_asset=False
):
    if dry_run:
        logging.info(
            f"[DELETE] 🧪 [DRY_RUN] Пропущено удаление: {component_name}:{component_version} (ID: {component_id})"
        )
        return

    endpoint = "assets" if use_asset else "components"
    url = f"{BASE_URL}service/rest/v1/{endpoint}/{component_id}"
    try:
        response = requests.delete(
            url, auth=(USER_NAME, PASSWORD), timeout=10, verify=False
        )
        response.raise_for_status()
        logging.info(
            f"[DELETE] ✅ Удалён: {component_name}:{component_version} (ID: {component_id})"
        )
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            logging.warning(
                f"[DELETE] ⚠️ Компонент не найден (404): {component_name}:{component_version} (ID: {component_id})"
            )
        else:
            logging.error(f"[DELETE] ❌ Ошибка HTTP при удалении {component_id}: {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"[DELETE] ❌ Ошибка при удалении {component_id}: {e}")


def get_matching_rule(
    version,
    regex_rules,
    no_match_retention,
    no_match_reserved,
    no_match_min_days_since_last_download,
):
    version_lower = version.lower()
    matched_rules = []

    for pattern, rules in regex_rules.items():
        if re.match(pattern, version_lower):
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

    retention = (
        timedelta(days=no_match_retention) if no_match_retention is not None else None
    )
    return (
        "no-match",
        retention,
        no_match_reserved,
        no_match_min_days_since_last_download,
    )


def filter_components_to_delete(
    components,
    regex_rules,
    no_match_retention,
    no_match_reserved,
    no_match_min_days_since_last_download,
):
    now_utc = datetime.now(timezone.utc)
    grouped = defaultdict(list)

    for component in components:
        version = component.get("version", "")
        name = component.get("name", "")
        assets = component.get("assets", [])
        if not assets or not version or not name:
            logging.info(
                f" ⏭ Пропуск: отсутствует имя, версия или assets у компонента {component}"
            )
            continue

        last_modified_strs = [a.get("lastModified") for a in assets if a.get("lastModified")]
        last_download_strs = [a.get("lastDownloaded") for a in assets if a.get("lastDownloaded")]

        if not last_modified_strs:
            logging.info(
                f" ⏭ Пропуск: отсутствует lastModified у компонента {name}:{version}"
            )
            continue

        try:
            last_modified = max(parse(s) for s in last_modified_strs)
        except Exception:
            logging.info(f" ⏭ Пропуск: ошибка парсинга lastModified у {name}:{version}")
            continue

        last_download = None
        if last_download_strs:
            try:
                last_download = max(parse(s) for s in last_download_strs)
            except Exception:
                logging.info(f" ⚠ Ошибка парсинга lastDownloaded у {name}:{version}")
                pass

        if version.lower() == "latest":
            logging.info(f" 🔒 Защищён от удаления (latest): {name}:{version}")
            continue

        pattern, retention, reserved, min_days_since_last_download = get_matching_rule(
            version,
            regex_rules,
            no_match_retention,
            no_match_reserved,
            no_match_min_days_since_last_download,
        )

        component.update(
            {
                "last_modified": last_modified,
                "last_download": last_download,
                "retention": retention,
                "reserved": reserved,
                "pattern": pattern,
                "min_days_since_last_download": min_days_since_last_download,
            }
        )

        grouped[(name, pattern)].append(component)

    to_delete = []

    for (name, pattern), group in grouped.items():
        sorted_group = sorted(group, key=lambda x: x["last_modified"], reverse=True)

        for i, component in enumerate(sorted_group):
            version = component.get("version", "Без версии")
            full_path = os.path.join(name, version).replace("\\", "/")
            age = now_utc - component["last_modified"]
            last_download = component.get("last_download")
            retention = component.get("retention")
            reserved = component.get("reserved")
            min_days_since_last_download = component.get("min_days_since_last_download")

            # Зарезервированные
            if reserved is not None and i < reserved:
                logging.info(
                    f" 📦 Зарезервирован: {full_path} | правило ({pattern}) (позиция {i + 1}/{reserved})"
                )
                continue

            # Проверка retention
            if retention is not None and age.days <= retention.days:
                logging.info(
                    f" 📦 Сохранён: {full_path} | правило ({pattern}) (retention: {age.days} дн. ≤ {retention.days})"
                )
                continue

            # Проверка скачиваний
            if last_download is not None and min_days_since_last_download is not None:
                days_since_download = (now_utc - last_download).days
                if days_since_download <= min_days_since_last_download:
                    logging.info(
                        f" 📦 Сохранён: {full_path} | правило ({pattern}) (скачивали {days_since_download} дн. назад ≤ {min_days_since_last_download})"
                    )
                    continue

            # Если не прошли проверки → удаляем
            reason = []
            if retention is not None:
                reason.append(f"retention: {age.days} дн. > {retention.days}")
            if last_download:
                reason.append(f"скачивали {(now_utc - last_download).days} дн. назад")
            else:
                reason.append("скачивали никогда")
            reason_text = ", ".join(reason)

            logging.info(
                f" 🗑 Удаление: {full_path} | правило ({pattern}) ({reason_text})"
            )
            to_delete.append(component)

    logging.info(f" 🧹 Обнаружено к удалению: {len(to_delete)} компонент(ов)")
    return to_delete



# ---------------------- MAVEN ----------------------

def detect_maven_type(component):
    """
    Определяет тип Maven-компонента (snapshot или release).
    """
    version = component.get("version", "").lower()

    # 1. Если явно содержит "snapshot" → snapshot
    if "snapshot" in version:
        return "snapshot"

    # 2. Timestamped snapshots (пример: 1.0-20250829.123456-1)
    timestamped_snapshot = re.match(r".*-\d{8}\.\d{6}-\d+", version)
    if timestamped_snapshot:
        return "snapshot"

    # 3. Всё остальное → release
    return "release"



def filter_maven_components_to_delete(components, maven_rules):
    now_utc = datetime.now(timezone.utc)
    grouped = defaultdict(list)

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
                pass

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

        component.update(
            {
                "last_modified": last_modified,
                "last_download": last_download,
                "retention": retention,
                "reserved": reserved,
                "pattern": pattern,
                "maven_type": maven_type,
                "min_days_since_last_download": min_days_since_last_download,
            }
        )

        grouped[(name, pattern, maven_type)].append(component)

    to_delete = []

    for (name, pattern, maven_type), group in grouped.items():
        sorted_group = sorted(group, key=lambda x: x["last_modified"], reverse=True)

        for i, component in enumerate(sorted_group):
            version = component.get("version", "Без версии")
            full_name = f"{name}:{version}"
            age = now_utc - component["last_modified"]
            last_download = component.get("last_download")
            retention = component.get("retention")
            reserved = component.get("reserved")
            min_days_since_last_download = component.get("min_days_since_last_download")

            if reserved is not None and i < reserved:
                logging.info(
                    f" 📦 Зарезервирован (Maven {maven_type}): {full_name} | правило ({pattern}) (позиция {i + 1}/{reserved})"
                )
                continue

            if retention is not None and age.days <= retention.days:
                logging.info(
                    f" 📦 Сохранён (Maven {maven_type}): {full_name} | правило ({pattern}) (retention: {age.days} дн. ≤ {retention.days})"
                )
                continue

            if last_download is not None and min_days_since_last_download is not None:
                days_since_download = (now_utc - last_download).days
                if days_since_download <= min_days_since_last_download:
                    logging.info(
                        f" 📦 Сохранён (Maven {maven_type}): {full_name} | правило ({pattern}) (скачивали {days_since_download} дн. назад ≤ {min_days_since_last_download})"
                    )
                    continue

            reason = []
            if retention is not None:
                reason.append(f"retention: {age.days} дн. > {retention.days}")
            if last_download:
                reason.append(f"скачивали {(now_utc - last_download).days} дн. назад")
            else:
                reason.append("скачивали никогда")
            reason_text = ", ".join(reason)

            logging.info(
                f" 🗑 Удаление (Maven {maven_type}): {full_name} | правило ({pattern}) ({reason_text})"
            )
            to_delete.append(component)

    logging.info(f" 🧹 Обнаружено к удалению (Maven): {len(to_delete)} компонент(ов)")
    return to_delete



# ---------------------- MAIN ----------------------

def clear_repository(repo_name, cfg):
    logging.info(f"\n🔄 Начало очистки репозитория: {repo_name}")

    repo_format = get_repository_format(repo_name)
    if not repo_format:
        logging.warning(f"⚠️ Пропущен репозиторий '{repo_name}' — неизвестный формат")
        return

    if repo_format not in ["raw", "docker", "maven2"]:
        logging.warning(
            f"⚠️ Репозиторий '{repo_name}' имеет неподдерживаемый формат '{repo_format}' и будет пропущен"
        )
        return

    items = get_repository_items(repo_name, repo_format)
    if not items:
        logging.info(f"ℹ️ Репозиторий '{repo_name}' пуст")
        return

    if repo_format == "raw":
        components = convert_raw_assets_to_components(items)
        to_delete = filter_components_to_delete(
            components,
            regex_rules=cfg.get("regex_rules", {}),
            no_match_retention=cfg.get("no_match_retention_days"),
            no_match_reserved=cfg.get("no_match_reserved", None),
            no_match_min_days_since_last_download=cfg.get(
                "no_match_min_days_since_last_download", None
            ),
        )
    elif repo_format == "maven2":
        components = items
        to_delete = filter_maven_components_to_delete(
            components, cfg.get("maven_rules", {})
        )
    else:  # docker
        components = items
        to_delete = filter_components_to_delete(
            components,
            regex_rules=cfg.get("regex_rules", {}),
            no_match_retention=cfg.get("no_match_retention_days"),
            no_match_reserved=cfg.get("no_match_reserved", None),
            no_match_min_days_since_last_download=cfg.get(
                "no_match_min_days_since_last_download", None
            ),
        )

    if not to_delete:
        logging.info(f"✅ Нет компонентов для удаления в '{repo_name}'")
        return

    logging.info(f"🚮 Удаление {len(to_delete)} компонент(ов)...")
    for component in to_delete:
        delete_component(
            component["id"],
            component.get("name", "Без имени"),
            component.get("version", "Без версии"),
            cfg.get("dry_run", False),
            use_asset=(repo_format == "raw"),
        )


def main():
    config_dir = os.path.join(os.path.dirname(__file__), "configs")
    config_files = []

    for root, _, files in os.walk(config_dir):
        for f in files:
            if f.endswith(".yaml") or f.endswith(".yml"):
                config_files.append(os.path.join(root, f))

    if not config_files:
        logging.warning("[MAIN] ⚠️ В папке 'configs/' и подкаталогах нет YAML-файлов")
        return

    for cfg_path in config_files:
        logging.info(f"\n📄 Обработка файла конфигурации: {cfg_path}")
        config = load_config(cfg_path)
        if not config:
            continue
        repos = config.get("repo_names", [])
        for repo in repos:
            clear_repository(repo, config)


if __name__ == "__main__":
    main()
