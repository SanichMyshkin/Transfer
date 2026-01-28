# excel_writer.py
import xlsxwriter
import logging
import sqlite3

logger = logging.getLogger(__name__)


def write_excel(data):
    wb = xlsxwriter.Workbook("zeus_report.xlsx")

    # -----------------------------------------
    # USERS
    # -----------------------------------------
    ws_u = wb.add_worksheet("Users")
    user_headers = ["project", "user", "displayName", "mail", "group", "cn"]
    for col, h in enumerate(user_headers):
        ws_u.write(0, col, h)

    all_users = []
    row = 1
    for p in data:
        for u in p["ad_users"]:
            ws_u.write(row, 0, p["name"])
            ws_u.write(row, 1, u["user"])
            ws_u.write(row, 2, u["displayName"])
            ws_u.write(row, 3, u["mail"])
            ws_u.write(row, 4, u["group"])
            ws_u.write(row, 5, u.get("cn", ""))
            all_users.append(u)
            row += 1

    # -----------------------------------------
    # UNIQ USERS
    # -----------------------------------------
    ws_uu = wb.add_worksheet("UniqUsers")

    uniq = {}
    for u in all_users:
        mail = u["mail"]
        if not mail:
            continue
        if mail not in uniq:
            uniq[mail] = {
                "user": u["user"],
                "displayName": u["displayName"],
                "mail": u["mail"],
                "groups": set(),
            }
        uniq[mail]["groups"].add(u["group"])

    uniq_users = list(uniq.values())

    uniq_headers = ["user", "displayName", "mail", "groups"]
    for col, h in enumerate(uniq_headers):
        ws_uu.write(0, col, h)

    row = 1
    for u in uniq_users:
        ws_uu.write(row, 0, u["user"])
        ws_uu.write(row, 1, u["displayName"])
        ws_uu.write(row, 2, u["mail"])
        ws_uu.write(row, 3, ", ".join(sorted(u["groups"])))
        row += 1

    # -----------------------------------------
    # BK USERS MATCH
    # -----------------------------------------
    ws_bk = wb.add_worksheet("BK_Users")

    logger.info("Читаем BK SQLite базу...")
    with sqlite3.connect("bk.sqlite") as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM Users").fetchall()

    bk_users_all = [dict(r) for r in rows]
    bk_by_email = {u["Email"].lower(): u for u in bk_users_all if u.get("Email")}

    matched_bk = []
    logger.info("Сопоставляем BK ↔ UniqUsers...")

    for u in uniq_users:
        mail = u["mail"].lower()
        if mail in bk_by_email:
            logger.info(f"✔ BK найден по email: {mail}")
            matched_bk.append(bk_by_email[mail])
        else:
            logger.info(f"⚠ BK не найден по email: {mail}")

    if matched_bk:
        headers = list(matched_bk[0].keys())
        for col, h in enumerate(headers):
            ws_bk.write(0, col, h)
        row = 1
        for r in matched_bk:
            for col, h in enumerate(headers):
                ws_bk.write(row, col, str(r[h]))
            row += 1
    else:
        ws_bk.write(0, 0, "Нет совпадений")

    # -----------------------------------------
    # MONITORING
    # -----------------------------------------
    ws_m = wb.add_worksheet("Monitoring")

    monitoring_headers = [
        "project",
        "team",
        "file",
        "metric_name",
        "enabled",
        "metricType",
        "interval",
        "telegram",
        "mail",
    ]
    for col, h in enumerate(monitoring_headers):
        ws_m.write(0, col, h)

    row = 1
    all_metrics = []
    for p in data:
        team = p["name"]

        for m in p["metrics"]:
            ws_m.write(row, 0, p["name"])
            ws_m.write(row, 1, team)
            ws_m.write(row, 2, m["file"])
            ws_m.write(row, 3, m["metric_name"])
            ws_m.write(row, 4, m["enabled"])
            ws_m.write(row, 5, m["metricType"])
            ws_m.write(row, 6, m["schedule_interval"])
            ws_m.write(row, 7, m["telegram"])
            ws_m.write(row, 8, m["mail"])
            all_metrics.append((team, m))
            row += 1

    # -----------------------------------------
    # TEAMS
    # -----------------------------------------
    ws_t = wb.add_worksheet("Teams")

    team_stats = {}

    for p in data:
        team = p["name"]

        if team not in team_stats:
            team_stats[team] = {
                "total_metrics": 0,
                "active_metrics": 0,
                "disabled_metrics": 0,
                "with_notifications": 0,
            }

        for m in p["metrics"]:
            team_stats[team]["total_metrics"] += 1
            if m["enabled"] is True:
                team_stats[team]["active_metrics"] += 1
            if m["enabled"] is False:
                team_stats[team]["disabled_metrics"] += 1
            if m.get("telegram") is True or m.get("mail") is True:
                team_stats[team]["with_notifications"] += 1

    headers = [
        "team",
        "metrics_total",
        "metrics_active",
        "metrics_disabled",
        "with_notifications",
    ]

    for col, h in enumerate(headers):
        ws_t.write(0, col, h)

    row = 1
    for team, st in team_stats.items():
        ws_t.write(row, 0, team)
        ws_t.write(row, 1, st["total_metrics"])
        ws_t.write(row, 2, st["active_metrics"])
        ws_t.write(row, 3, st["disabled_metrics"])
        ws_t.write(row, 4, st["with_notifications"])
        row += 1

    # -----------------------------------------
    # SUMMARY
    # -----------------------------------------
    ws_s = wb.add_worksheet("Summary")

    total_metrics = sum(st["total_metrics"] for st in team_stats.values())
    active_metrics = sum(st["active_metrics"] for st in team_stats.values())
    disabled_metrics = sum(st["disabled_metrics"] for st in team_stats.values())
    with_notifications = sum(st["with_notifications"] for st in team_stats.values())

    rows = [
        ("Total metrics", total_metrics),
        ("Active metrics", active_metrics),
        ("Disabled metrics", disabled_metrics),
        ("Metrics with notifications", with_notifications),
        ("Teams count", len(team_stats)),
        ("Total users", len(all_users)),
        ("Unique users", len(uniq_users)),
        ("Matched BK users", len(matched_bk)),
    ]

    ws_s.write(0, 0, "Metric")
    ws_s.write(0, 1, "Value")

    for i, (k, v) in enumerate(rows, start=1):
        ws_s.write(i, 0, k)
        ws_s.write(i, 1, v)

    wb.close()
    logger.info("Excel отчет создан: zeus_report.xlsx")
