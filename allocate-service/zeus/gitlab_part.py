import os
import json
import logging

import gitlab
import yaml
from dotenv import load_dotenv

from ldap_part import ldap_connect, get_project_ad_users

load_dotenv()
logger = logging.getLogger(__name__)

GITLAB_URL = os.getenv("GITLAB_URL")
TOKEN = os.getenv("TOKEN")
GROUP_ID = os.getenv("GROUP_ID")


def gitlab_connect():
    logger.info("Подключаемся к GitLab...")
    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN, ssl_verify=False, timeout=60)
    gl.auth()
    logger.info("GitLab: подключение успешно")
    return gl


def get_group_projects(gl):
    group = gl.groups.get(GROUP_ID)
    projs = group.projects.list(all=True, include_subgroups=True)
    return projs


def get_monitoring_files(gl, project_id: int, project_name: str):
    logger.info(f"[{project_name}] Поиск monitoring файлов...")
    res = []
    proj = gl.projects.get(project_id)
    root = proj.repository_tree(all=True)
    zeus = next(
        (i for i in root if i["type"] == "tree" and i["name"].startswith("zeus-")),
        None,
    )
    if not zeus:
        logger.info(f"[{project_name}] zeus-* не найдено")
        return []
    logger.info(f"[{project_name}] Найдена папка: {zeus['name']}")
    zeus_items = proj.repository_tree(path=zeus["path"], all=True)
    subfolders = [i for i in zeus_items if i["type"] == "tree"]
    logger.info(f"[{project_name}] Подпапок: {len(subfolders)}")

    for sub in subfolders:
        sub_items = proj.repository_tree(path=sub["path"], all=True)
        items = [
            f
            for f in sub_items
            if f["type"] == "blob"
            and (
                f["name"].lower().endswith("-monitors.yaml")
                or f["name"].lower().endswith("-monitors.yml")
            )
        ]
        for f in items:
            logger.info(f"[{project_name}] Файл: {sub['path']}/{f['name']}")
            res.append(
                {
                    "project_name": project_name,
                    "file_name": f["name"],
                    "file_path": f["path"],
                    "folder": sub["path"],
                }
            )

    if not res:
        logger.info(f"[{project_name}] Monitoring файлов не найдено")
    return res


def get_gitlab_json_prefix(gl, project_id: int):
    proj = gl.projects.get(project_id)
    root = proj.repository_tree(all=True)
    gitlab_dir = next(
        (i for i in root if i["type"] == "tree" and i["name"] == "gitlab"), None
    )
    if not gitlab_dir:
        return None
    lvl2 = proj.repository_tree(path=gitlab_dir["path"], all=True)
    mapping = next(
        (i for i in lvl2 if i["type"] == "tree" and i["name"] == "mapping"), None
    )
    if not mapping:
        return None
    items = proj.repository_tree(path=mapping["path"], all=True)
    jf = next(
        (i for i in items if i["type"] == "blob" and i["name"] == "gitlab.json"), None
    )
    if not jf:
        return None
    file_data = proj.files.get(file_path=jf["path"], ref="main")
    content = file_data.decode().decode("utf-8")
    data = json.loads(content)
    prefix = data.get("group", {}).get("name")
    logger.info(f"Префикс из gitlab.json: {prefix}")
    return prefix


def get_file_content(gl, project_id: int, path: str) -> str:
    proj = gl.projects.get(project_id)
    f = proj.files.get(file_path=path, ref="main")
    return f.decode().decode("utf-8")


def parse_monitoring_yaml(content: str, project: str, file_name: str):
    try:
        data = yaml.safe_load(content)
    except Exception as e:
        logger.error(f"[{project}] Ошибка YAML в {file_name}: {e}")
        return []

    listing = (
        data.get("zeus", {})
        .get("monitors", {})
        .get("listing", [])
    )

    logger.info(f"[{project}] Мониторов в {file_name}: {len(listing)}")
    metrics = []

    for m in listing:
        name = m.get("name")
        enabled = m.get("enabled")

        if m.get("metricType"):
            mtype = m.get("metricType")
        else:
            mtype = m.get("type")

        sched = m.get("schedule", {}).get("timerProperties", {})
        interval = sched.get("interval")

        notify = m.get("notifications", {}).get("sendersStatus", {})
        mail = notify.get("mail")
        telegram = notify.get("telegram")

        metrics.append(
            {
                "project": project,
                "file": file_name,
                "metric_name": name,
                "metricType": mtype,
                "enabled": enabled,
                "schedule_interval": interval,
                "telegram": telegram,
                "mail": mail,
            }
        )

        logger.info(
            f"- {name}: enabled={enabled}, type={mtype}, interval={interval}, "
            f"tg={telegram}, mail={mail}"
        )

    return metrics


def process_projects(gl, projects):
    conn = ldap_connect()
    result = []
    logger.info("Старт обработки проектов...")

    for p in projects:
        logger.info(f"\n=== Проект: {p.name} ===")
        files = get_monitoring_files(gl, p.id, p.name)
        if not files:
            logger.info(f"[{p.name}] Пропущен (нет monitoring файлов)")
            continue

        prefix = get_gitlab_json_prefix(gl, p.id)
        logger.info(f"[{p.name}] prefix={prefix}")

        ad_users = get_project_ad_users(conn, prefix)
        logger.info(f"[{p.name}] Найдено AD пользователей: {len(ad_users)}")
        for u in ad_users:
            logger.info(f"[{p.name}] AD: {u['user']} | {u['mail']} | {u['group']}")

        metrics = []
        for f in files:
            raw = get_file_content(gl, p.id, f["file_path"])
            parsed = parse_monitoring_yaml(raw, p.name, f["file_name"])
            metrics.extend(parsed)

        active_cnt = sum(1 for m in metrics if m.get("enabled") is True)
        disabled_cnt = sum(1 for m in metrics if m.get("enabled") is False)
        logger.info(
            f"[{p.name}] Метрик всего: {len(metrics)}, активных: {active_cnt}, отключенных: {disabled_cnt}"
        )

        result.append(
            {
                "id": p.id,
                "name": p.name,
                "files": files,
                "gitlab_group_name": prefix,
                "ad_users": ad_users,
                "metrics": metrics,
            }
        )

    conn.unbind()
    logger.info("Обработка проектов завершена.")
    return result
