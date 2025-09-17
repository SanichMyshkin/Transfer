import os
import logging
import requests
from datetime import datetime, timezone
from dateutil.parser import parse
from collections import defaultdict
from dotenv import load_dotenv
import urllib3

from common import get_matching_rule
from maven import filter_maven_components_to_delete

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

USER_NAME = os.getenv("USER_NAME")
PASSWORD = os.getenv("PASSWORD")
BASE_URL = os.getenv("BASE_URL")


# ===== API ВСПОМОГАТЕЛЬНЫЕ =====
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
                url,
                auth=(USER_NAME, PASSWORD),
                params=params,
                timeout=10,
                verify=False,
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


def filter_components_to_delete(
    components,
    regex_rules,
    no_match_retention,
    no_match_reserved,
    no_match_min_days_since_last_download,
):
    now_utc = datetime.now(timezone.utc)
    grouped = defaultdict(list)
    no_match_list = []

    def _days(x):
        if x is None:
            return None
        if hasattr(x, "days"):
            try:
                return int(x.days)
            except Exception:
                pass
        try:
            return int(x)
        except Exception:
            return None

    def _to_int(x):
        if x is None:
            return None
        try:
            return int(x)
        except Exception:
            return None

    # ===== Шаг 1: собираем компоненты =====
    for component in components:
        version = component.get("version", "")
        name = component.get("name", "")
        assets = component.get("assets", [])
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
                last_download = None

        component.update({"last_modified": last_modified, "last_download": last_download})

        if isinstance(version, str) and version.lower() == "latest":
            component.update({
                "pattern": "latest",
                "will_delete": False,
                "delete_reason": "версия 'latest' — сохраняем",
            })
            continue

        pattern, retention, reserved, min_days = get_matching_rule(
            version, regex_rules, no_match_retention, no_match_reserved, no_match_min_days_since_last_download
        )

        retention_days = _days(retention)
        reserved_count = _to_int(reserved)
        min_days_int = _to_int(min_days)

        component.update({
            "pattern": pattern,
            "retention_days": retention_days,
            "reserved_count": reserved_count,
            "min_days_since_last_download": min_days_int,
        })

        if pattern == "no-match":
            no_match_list.append(component)
        else:
            grouped[(name, pattern)].append(component)

    saved = []
    to_delete = []

    # ===== Шаг 2: обработка no-match =====
    if no_match_list:
        # Если для no-match нет правил — сохраняем и не помечаем на удаление
        if no_match_retention is None and no_match_reserved is None and no_match_min_days_since_last_download is None:
            for comp in no_match_list:
                comp["will_delete"] = False
                comp["delete_reason"] = "нет правил no-match → сохраняем"
                saved.append(comp)
        else:
            sorted_no_match = sorted(no_match_list, key=lambda x: x["last_modified"], reverse=True)
            for i, comp in enumerate(sorted_no_match):
                age = now_utc - comp["last_modified"]
                last_download = comp.get("last_download")
                retention_days = comp.get("retention_days")
                reserved_count = comp.get("reserved_count") or 0
                min_days = comp.get("min_days_since_last_download")

                if reserved_count and i < reserved_count:
                    comp["will_delete"] = False
                    comp["delete_reason"] = f"зарезервирован (позиция {i + 1}/{reserved_count}, no-match)"
                    saved.append(comp)
                    continue

                if retention_days is not None and age.days <= retention_days:
                    comp["will_delete"] = False
                    comp["delete_reason"] = f"свежий (возраст {age.days} дн. ≤ {retention_days} дн., no-match)"
                    saved.append(comp)
                    continue

                if min_days is not None and last_download and (now_utc - last_download).days <= min_days:
                    comp["will_delete"] = False
                    comp["delete_reason"] = f"недавно скачивали ({(now_utc - last_download).days} дн. ≤ {min_days} дн., no-match)"
                    saved.append(comp)
                    continue

                comp["will_delete"] = True
                comp["delete_reason"] = f"удаляется по правилам no-match"

    # ===== Шаг 3: обработка групп с regex =====
    for (name, pattern), group in grouped.items():
        sorted_group = sorted(group, key=lambda x: x["last_modified"], reverse=True)
        for i, comp in enumerate(sorted_group):
            age = now_utc - comp["last_modified"]
            last_download = comp.get("last_download")
            retention_days = comp.get("retention_days")
            reserved_count = comp.get("reserved_count") or 0
            min_days = comp.get("min_days_since_last_download")

            if reserved_count and i < reserved_count:
                comp["will_delete"] = False
                comp["delete_reason"] = f"зарезервирован (позиция {i + 1}/{reserved_count}, правило '{pattern}')"
                saved.append(comp)
                continue

            if retention_days is not None and age.days <= retention_days:
                comp["will_delete"] = False
                comp["delete_reason"] = f"свежий (возраст {age.days} дн. ≤ {retention_days} дн., правило '{pattern}')"
                saved.append(comp)
                continue

            if min_days is not None and last_download and (now_utc - last_download).days <= min_days:
                comp["will_delete"] = False
                comp["delete_reason"] = f"недавно скачивали ({(now_utc - last_download).days} дн. ≤ {min_days} дн., правило '{pattern}')"
                saved.append(comp)
                continue

            comp["will_delete"] = True
            comp["delete_reason"] = f"удаляется по правилу '{pattern}'"

    return to_delete



# ===== ОЧИСТКА РЕПОЗИТОРИЯ =====
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
