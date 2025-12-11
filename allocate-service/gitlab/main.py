import os
import logging
import gitlab
import urllib3
import xlsxwriter
import sqlite3
import humanize
import re
from dotenv import load_dotenv
from pathlib import Path
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
BK_SQLITE_PATH = os.getenv("BK_SQLITE_PATH")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


def get_gitlab_connection():
    logger.info("Подключаемся к GitLab...")
    gl = gitlab.Gitlab(
        GITLAB_URL, private_token=GITLAB_TOKEN, ssl_verify=False, timeout=60
    )
    gl.auth()
    logger.info("Успешное подключение к GitLab")
    return gl


def get_users(gl):
    logger.info("Получаем пользователей GitLab...")
    users = gl.users.list(all=True, iterator=True)
    result = []

    for u in users:
        extern_uid = ""
        identities = getattr(u, "identities", [])
        if identities and isinstance(identities, list):
            extern_uid = ", ".join(
                i.get("extern_uid", "") for i in identities if isinstance(i, dict)
            )

        result.append(
            {
                "id": u.id,
                "username": u.username,
                "email": getattr(u, "email", ""),
                "name": u.name,
                "last_sign_in_at": getattr(u, "last_sign_in_at", ""),
                "last_activity_on": getattr(u, "last_activity_on", ""),
                "extern_uid": extern_uid,
            }
        )

    logger.info(f"Пользователей получено: {len(result)}")
    return result


def get_stat(gl):
    logger.info("Получаем статистику GitLab...")
    stats = gl.statistics.get()

    fields = {
        "forks": stats.forks,
        "issues": stats.issues,
        "merge_requests": stats.merge_requests,
        "notes": stats.notes,
        "snippets": stats.snippets,
        "ssh_keys": stats.ssh_keys,
        "milestones": stats.milestones,
        "users": stats.users,
        "projects": stats.projects,
        "groups": stats.groups,
        "active_users": stats.active_users,
    }

    norm = {}
    for k, v in fields.items():
        if isinstance(v, str):
            v = v.replace(",", "").strip()
            v = int(v) if v.isdigit() else 0
        norm[k] = v

    logger.info("Статистика успешно получена")
    return norm


def get_projects_stats(gl):
    logger.info("Собираем статистику проектов...")
    projects = gl.projects.list(all=True, iterator=True)
    result = []
    total_commits = 0

    for idx, project in enumerate(projects, start=1):
        try:
            full = gl.projects.get(project.id, statistics=True)
            stats = getattr(full, "statistics", {}) or {}

            commits = stats.get("commit_count", 0)
            if isinstance(commits, int):
                total_commits += commits

            result.append(
                {
                    "id": full.id,
                    "name": full.name,
                    "path_with_namespace": full.path_with_namespace,
                    "repository_size": humanize.naturalsize(
                        stats.get("repository_size", 0), binary=True
                    ),
                    "lfs_objects_size": humanize.naturalsize(
                        stats.get("lfs_objects_size", 0), binary=True
                    ),
                    "job_artifacts_size": humanize.naturalsize(
                        stats.get("job_artifacts_size", 0), binary=True
                    ),
                    "storage_size": humanize.naturalsize(
                        stats.get("storage_size", 0), binary=True
                    ),
                    "commit_count": commits,
                    "last_activity_at": full.last_activity_at,
                    "visibility": full.visibility,
                }
            )

            if idx % 50 == 0:
                logger.info(f"Обработано проектов: {idx}")

            time.sleep(0.05)

        except Exception as e:
            logger.warning(f"Ошибка проекта {project.id}: {e}")
            continue

    result.sort(key=lambda x: x.get("storage_size", ""), reverse=True)
    logger.info(f"Проектов: {len(result)}, коммитов: {total_commits}")
    return result, total_commits


def get_runners_info(gl):
    logger.info("Получаем раннеры...")
    runners = gl.runners_all.list(all=True)
    data = []

    for r in runners:
        try:
            full = gl.runners.get(r.id)
            desc = full.description or f"runner-{r.id}"

            src_name, src_path = "", ""

            if full.runner_type == "group_type":
                groups = getattr(full, "groups", [])
                if groups:
                    g = groups[0]
                    src_name = g.get("name", "")
                    src_path = g.get("full_path", "")

            elif full.runner_type == "project_type":
                projects = getattr(full, "projects", [])
                if projects:
                    p = projects[0]
                    src_name = p.get("name", "")
                    src_path = p.get("path_with_namespace", "")

            data.append(
                {
                    "id": full.id,
                    "source_name": src_name,
                    "source_path": src_path,
                    "runner_type": full.runner_type,
                    "description": desc,
                    "status": getattr(full, "status", ""),
                    "online": getattr(full, "online", None),
                    "ip_address": getattr(full, "ip_address", ""),
                    "tag_list": ", ".join(getattr(full, "tag_list", []) or []),
                    "contacted_at": getattr(full, "contacted_at", ""),
                }
            )

        except Exception as e:
            logger.warning(f"Ошибка раннера {r.id}: {e}")

        time.sleep(0.05)

    logger.info(f"Всего раннеров: {len(data)}")
    return data, len(data)


def load_bk_users():
    logger.info("Загружаем BK SQLite...")
    conn = sqlite3.connect(BK_SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM bk").fetchall()
    conn.close()
    logger.info(f"BK пользователей: {len(rows)}")
    return [dict(r) for r in rows]


def match_users(gitlab_users, bk_users):
    logger.info("Сопоставляем пользователей по логину и email...")

    bk_by_login = {(u.get("sAMAccountName") or "").strip().lower(): u for u in bk_users}
    bk_by_email = {(u.get("Email") or "").strip().lower(): u for u in bk_users}

    matched = []
    unmatched = []

    for u in gitlab_users:
        gl_login = (u.get("username") or "").strip().lower()
        gl_email = (u.get("email") or "").strip().lower()

        found = None

        if gl_login:
            found = bk_by_login.get(gl_login)

        if not found and gl_email:
            found = bk_by_email.get(gl_email)

        if found:
            matched.append(found)
        else:
            unmatched.append(u)

    logger.info(f"Совпадений: {len(matched)}, не найдено: {len(unmatched)}")
    return matched, unmatched


def split_unmatched(unmatched):
    tech = []
    fired = []

    for u in unmatched:
        eu = (u.get("extern_uid") or "").strip()

        if eu == "":
            tech.append(u)
            continue

        m = re.search(r"cn=([^,]+)", eu, re.IGNORECASE)
        if not m:
            tech.append(u)
            continue

        cn = m.group(1).strip()

        if not re.search(r"[а-яА-ЯёЁ]", cn):
            tech.append(u)
            continue

        fired.append(u)

    return tech, fired


def write_to_excel(
    gitlab_users,
    stats,
    projects,
    runners,
    bk_matched,
    tech_users,
    fired_users,
    filename="gitlab_report.xlsx",
):
    filename = str(Path(filename).resolve())
    wb = xlsxwriter.Workbook(filename)

    sh = wb.add_worksheet("Пользователи")
    headers = [
        "id",
        "username",
        "email",
        "name",
        "last_sign_in_at",
        "last_activity_on",
        "extern_uid",
    ]
    for c, h in enumerate(headers):
        sh.write(0, c, h)
    for r, u in enumerate(gitlab_users, start=1):
        sh.write_row(r, 0, [u.get(h, "") for h in headers])

    sh2 = wb.add_worksheet("Совпавшие")
    if bk_matched:
        headers2 = list(bk_matched[0].keys())
        for c, h in enumerate(headers2):
            sh2.write(0, c, h)
        for r, u in enumerate(bk_matched, start=1):
            sh2.write_row(r, 0, [u.get(h, "") for h in headers2])

    sh3 = wb.add_worksheet("Технические")
    for c, h in enumerate(headers):
        sh3.write(0, c, h)
    for r, u in enumerate(tech_users, start=1):
        sh3.write_row(r, 0, [u.get(h, "") for h in headers])

    sh4 = wb.add_worksheet("Уволенные")
    for c, h in enumerate(headers):
        sh4.write(0, c, h)
    for r, u in enumerate(fired_users, start=1):
        sh4.write_row(r, 0, [u.get(h, "") for h in headers])

    sh5 = wb.add_worksheet("Статистика")
    sh5.write(0, 0, "Показатель")
    sh5.write(0, 1, "Значение")
    for r, (k, v) in enumerate(stats.items(), start=1):
        sh5.write(r, 0, k)
        sh5.write(r, 1, v)

    sh6 = wb.add_worksheet("Проекты")
    proj_headers = [
        "id",
        "name",
        "path_with_namespace",
        "repository_size",
        "lfs_objects_size",
        "job_artifacts_size",
        "storage_size",
        "commit_count",
        "last_activity_at",
        "visibility",
    ]
    for c, h in enumerate(proj_headers):
        sh6.write(0, c, h)
    for r, p in enumerate(projects, start=1):
        sh6.write_row(r, 0, [p.get(h, "") for h in proj_headers])

    sh7 = wb.add_worksheet("Раннеры")
    runner_headers = [
        "id",
        "source_name",
        "source_path",
        "runner_type",
        "description",
        "status",
        "online",
        "ip_address",
        "tag_list",
        "contacted_at",
    ]
    for c, h in enumerate(runner_headers):
        sh7.write(0, c, h)
    for r, p in enumerate(runners, start=1):
        sh7.write_row(r, 0, [p.get(h, "") for h in runner_headers])

    wb.close()
    logger.info(f"Excel сохранён: {filename}")
    return filename


def main():
    logger.info("========== СТАРТ GitLab ОТЧЁТА ==========")

    gl = get_gitlab_connection()
    gitlab_users = get_users(gl)
    stats = get_stat(gl)
    projects, commits = get_projects_stats(gl)
    runners, runners_count = get_runners_info(gl)

    stats["total_commits"] = commits
    stats["projects_processed"] = len(projects)
    stats["runners_total"] = runners_count

    bk_users = load_bk_users()
    bk_matched, unmatched = match_users(gitlab_users, bk_users)
    tech, fired = split_unmatched(unmatched)

    write_to_excel(gitlab_users, stats, projects, runners, bk_matched, tech, fired)

    logger.info("Готово.")


if __name__ == "__main__":
    main()
