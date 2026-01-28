# main.py
import os
import json
import logging
import urllib3
from io import StringIO

import gitlab
from dotenv import load_dotenv

from ruamel.yaml import YAML

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gitlab_metrics_report")

load_dotenv()

GITLAB_URL = os.getenv("GITLAB_URL", "").rstrip("/")
TOKEN = os.getenv("TOKEN", "")
GROUP_ID = os.getenv("GROUP_ID", "")

OUTPUT_XLSX = os.getenv("OUTPUT_XLSX", "gitlab_metrics_report.xlsx")
GIT_REF = os.getenv("GIT_REF", "main")  # если у вас master — поменяй

# Важно: YAML не допускает табы в отступах. Мы их нормализуем.
TAB_REPLACEMENT = os.getenv("TAB_REPLACEMENT", "  ")  # по умолчанию 2 пробела


def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"Не задано окружение {name}")
    return v


def gl_connect():
    must_env("GITLAB_URL")
    must_env("TOKEN")
    must_env("GROUP_ID")

    log.info("Подключаемся к GitLab...")
    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN, ssl_verify=False, timeout=60)
    gl.auth()
    log.info("GitLab: ok")
    return gl


def get_group_projects(gl):
    log.info(f"Получаем проекты группы GROUP_ID={GROUP_ID} (включая сабгруппы)...")
    group = gl.groups.get(GROUP_ID)
    projs = group.projects.list(all=True, include_subgroups=True)
    log.info(f"Проектов найдено: {len(projs)}")
    return projs


def repo_tree(proj, path=None):
    if path:
        return proj.repository_tree(path=path, all=True)
    return proj.repository_tree(all=True)


def get_file_text(proj, file_path: str, ref: str) -> str:
    f = proj.files.get(file_path=file_path, ref=ref)
    return f.decode().decode("utf-8")


def get_team_name_from_gitlab_json(proj, ref: str) -> str | None:
    # gitlab/mapping/gitlab.json -> group.name
    try:
        root = repo_tree(proj)
        gitlab_dir = next((i for i in root if i["type"] == "tree" and i["name"] == "gitlab"), None)
        if not gitlab_dir:
            return None

        lvl2 = repo_tree(proj, gitlab_dir["path"])
        mapping = next((i for i in lvl2 if i["type"] == "tree" and i["name"] == "mapping"), None)
        if not mapping:
            return None

        items = repo_tree(proj, mapping["path"])
        jf = next((i for i in items if i["type"] == "blob" and i["name"] == "gitlab.json"), None)
        if not jf:
            return None

        txt = get_file_text(proj, jf["path"], ref)
        data = json.loads(txt)
        name = (data.get("group", {}) or {}).get("name")
        name = (name or "").strip()
        return name or None
    except Exception as e:
        log.warning(f"[{proj.path_with_namespace}] gitlab.json не прочитан: {e}")
        return None


def find_monitoring_files(proj):
    # ищем в zeus-* подпапках файлы *-monitors.yml(yaml)
    try:
        root = repo_tree(proj)
    except Exception as e:
        log.warning(f"[{proj.path_with_namespace}] repository_tree error: {e}")
        return []

    zeus_dir = next(
        (i for i in root if i["type"] == "tree" and i["name"].startswith("zeus-")),
        None,
    )
    if not zeus_dir:
        return []

    files = []
    try:
        zeus_items = repo_tree(proj, zeus_dir["path"])
        subfolders = [i for i in zeus_items if i["type"] == "tree"]
    except Exception as e:
        log.warning(f"[{proj.path_with_namespace}] zeus-* tree error: {e}")
        return []

    for sub in subfolders:
        try:
            sub_items = repo_tree(proj, sub["path"])
        except Exception as e:
            log.warning(f"[{proj.path_with_namespace}] tree({sub['path']}) error: {e}")
            continue

        for f in sub_items:
            if f["type"] != "blob":
                continue
            n = (f.get("name") or "").lower()
            if n.endswith("-monitors.yaml") or n.endswith("-monitors.yml"):
                files.append(f["path"])

    return files


def _ruamel_yaml():
    y = YAML(typ="rt")  # round-trip: комментарии/порядок сохраняются
    y.preserve_quotes = True
    # если захочешь реально форматировать обратно в YAML — эти отступы пригодятся:
    y.indent(mapping=2, sequence=4, offset=2)
    y.width = 4096
    return y


def normalize_yaml_text(raw: str) -> str:
    # Минимальная нормализация: только табы -> пробелы.
    # Комментарии ruamel.yaml понимает, проблема у тебя именно в табах.
    if "\t" in raw:
        return raw.replace("\t", TAB_REPLACEMENT)
    return raw


def parse_monitors_with_ruamel(raw: str, project: str, file_path: str):
    y = _ruamel_yaml()
    text = normalize_yaml_text(raw)

    try:
        data = y.load(text)
    except Exception as e:
        log.warning(f"[{project}] YAML пропущен (не парсится даже после tab-fix): {file_path} ({e})")
        return []

    if not isinstance(data, dict):
        return []

    zeus = data.get("zeus") or {}
    monitors = zeus.get("monitors") or {}
    listing = monitors.get("listing") or []
    if not isinstance(listing, list):
        return []

    out = []
    for m in listing:
        if not isinstance(m, dict):
            continue

        enabled = m.get("enabled")
        notify = (m.get("notifications") or {}).get("sendersStatus") or {}
        telegram = notify.get("telegram") is True
        mail = notify.get("mail") is True

        out.append(
            {
                "enabled": enabled,
                "has_notifications": telegram or mail,
            }
        )

    return out


def build_report_rows(gl, projects, ref: str):
    stats = {}  # team -> counters
    processed = 0
    with_mon_files = 0
    skipped_yaml_files = 0

    for p in projects:
        processed += 1
        try:
            proj = gl.projects.get(p.id)
        except Exception as e:
            log.warning(f"[{p.name}] Не смог получить project объект: {e}")
            continue

        mon_files = find_monitoring_files(proj)
        if not mon_files:
            continue

        with_mon_files += 1

        team = get_team_name_from_gitlab_json(proj, ref) or p.name
        if team not in stats:
            stats[team] = {
                "active": 0,
                "disabled": 0,
                "notifications": 0,
                "total": 0,
            }

        log.info(f"[{p.name}] service='{team}' files={len(mon_files)}")

        for path in mon_files:
            try:
                raw = get_file_text(proj, path, ref)
            except Exception as e:
                log.warning(f"[{p.name}] Не смог прочитать {path} ({ref}): {e}")
                continue

            monitors = parse_monitors_with_ruamel(raw, p.name, path)
            if not monitors:
                skipped_yaml_files += 1
                continue

            for m in monitors:
                stats[team]["total"] += 1
                if m["enabled"] is True:
                    stats[team]["active"] += 1
                elif m["enabled"] is False:
                    stats[team]["disabled"] += 1
                if m["has_notifications"]:
                    stats[team]["notifications"] += 1

    total_metrics = sum(v["total"] for v in stats.values())

    rows = []
    for team, v in stats.items():
        pct = (v["total"] / total_metrics * 100.0) if total_metrics > 0 else 0.0
        rows.append(
            {
                "service": team,
                "active": v["active"],
                "disabled": v["disabled"],
                "notifications": v["notifications"],
                "total": v["total"],
                "pct": pct,
            }
        )

    rows.sort(key=lambda r: r["total"], reverse=True)

    log.info(f"Проектов обработано: {processed}, с monitoring файлами: {with_mon_files}")
    log.info(f"Файлов YAML пропущено (не распарсились/пустые listing): {skipped_yaml_files}")
    log.info(f"Сервисов в отчете: {len(rows)}, всего метрик: {total_metrics}")
    return rows, total_metrics


def write_excel(rows, total_metrics: int, out_file: str):
    wb = Workbook()
    ws = wb.active
    ws.title = "Report"

    headers = [
        "Наименование сервиса",
        "кол-во метрик активных",
        "кол-во метрик выключеных",
        "уведомления",
        "Сумма метрик всего",
        "% потребления от общего числа метрик",
    ]

    bold = Font(bold=True)
    for col, h in enumerate(headers, start=1):
        c = ws.cell(row=1, column=col, value=h)
        c.font = bold

    for i, r in enumerate(rows, start=2):
        ws.cell(i, 1, r["service"])
        ws.cell(i, 2, r["active"])
        ws.cell(i, 3, r["disabled"])
        ws.cell(i, 4, r["notifications"])
        ws.cell(i, 5, r["total"])
        ws.cell(i, 6, round(r["pct"], 2))

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{max(1, len(rows) + 1)}"

    widths = [44, 24, 26, 16, 20, 34]
    for idx, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(idx)].width = w

    last = len(rows) + 2
    ws.cell(last, 1, "ИТОГО").font = bold
    ws.cell(last, 2, sum(r["active"] for r in rows)).font = bold
    ws.cell(last, 3, sum(r["disabled"] for r in rows)).font = bold
    ws.cell(last, 4, sum(r["notifications"] for r in rows)).font = bold
    ws.cell(last, 5, total_metrics).font = bold
    ws.cell(last, 6, 100.0 if total_metrics > 0 else 0.0).font = bold

    wb.save(out_file)
    log.info(f"Excel отчет создан: {out_file}")


def main():
    log.info("=== START: GitLab metrics report (ruamel.yaml + openpyxl, без LDAP) ===")
    gl = gl_connect()
    projects = get_group_projects(gl)
    rows, total = build_report_rows(gl, projects, ref=GIT_REF)
    write_excel(rows, total, OUTPUT_XLSX)
    log.info("=== DONE ===")


if __name__ == "__main__":
    main()
