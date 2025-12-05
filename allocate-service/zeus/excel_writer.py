import logging
import sqlite3
import xlsxwriter

logger = logging.getLogger(__name__)

def write_excel(data, prefixes=None):
    wb = xlsxwriter.Workbook("zeus_report.xlsx")

    ws = wb.add_worksheet("Users")
    headers = ["project", "gitlab_group", "ad_user", "displayName", "mail", "cn"]
    for col, h in enumerate(headers):
        ws.write(0, col, h)

    row = 1
    all_users = []
    team_stats = {}

    for p in data:
        prefix = p.get("gitlab_group_name", "") or ""
        team = prefix.replace("*", "") if prefix else ""
        if team:
            st = team_stats.setdefault(
                team,
                {"projects": set(), "total": 0, "active": 0, "disabled": 0},
            )
            st["projects"].add(p["name"])
            for m in p.get("metrics", []):
                st["total"] += 1
                if m.get("enabled") is True:
                    st["active"] += 1
                elif m.get("enabled") is False:
                    st["disabled"] += 1

        for u in p["ad_users"]:
            ws.write(row, 0, p["name"])
            ws.write(row, 1, prefix)
            ws.write(row, 2, u.get("user", ""))
            ws.write(row, 3, u.get("displayName", ""))
            ws.write(row, 4, u.get("mail", ""))
            ws.write(row, 5, u.get("cn", ""))
            all_users.append(u)
            row += 1

    ws_u = wb.add_worksheet("UniqUsers")
    headers_u = ["user", "displayName", "mail", "cn", "ad_groups"]
    for col, h in enumerate(headers_u):
        ws_u.write(0, col, h)

    uniq = {}

    for u in all_users:
        email = u.get("mail", "").lower()
        if not email:
            continue

        if email not in uniq:
            uniq[email] = {
                "user": u.get("user", ""),
                "displayName": u.get("displayName", ""),
                "mail": email,
                "cn": u.get("cn", ""),
                "groups": set(),
            }

        uniq[email]["groups"].add(u.get("group", ""))

    row = 1
    for item in uniq.values():
        ws_u.write(row, 0, item["user"])
        ws_u.write(row, 1, item["displayName"])
        ws_u.write(row, 2, item["mail"])
        ws_u.write(row, 3, item["cn"])
        ws_u.write(row, 4, ", ".join(sorted(item["groups"])))
        row += 1

    logger.info("Opening BK SQLite database...")

    with sqlite3.connect("bk.sqlite") as conn_bk:
        conn_bk.row_factory = sqlite3.Row
        bk_rows = conn_bk.execute("SELECT * FROM bk").fetchall()

    bk_users = [dict(r) for r in bk_rows]
    logger.info(f"BK users loaded: {len(bk_users)}")

    bk_email_map = {(u.get("Email") or "").strip().lower(): u for u in bk_users}

    matched = []
    logger.info("Matching BK users with unique AD users...")

    for u in uniq.values():
        email = (u.get("mail") or "").strip().lower()
        if not email:
            logger.info("✖ SKIP: empty email in UniqUsers")
            continue

        if email in bk_email_map:
            logger.info(f"✔ BK match found: {email}")
            matched.append(bk_email_map[email])
        else:
            logger.info(f"✖ BK NOT FOUND: {email}")

    logger.info(f"BK matched total: {len(matched)}")

    ws_bk = wb.add_worksheet("BK_Users")

    if matched:
        headers_bk = list(matched[0].keys())
        for col, h in enumerate(headers_bk):
            ws_bk.write(0, col, h)

        row = 1
        for user in matched:
            for col, h in enumerate(headers_bk):
                ws_bk.write(row, col, str(user.get(h, "")))
            row += 1
    else:
        ws_bk.write(0, 0, "NO MATCHED BK USERS")

    ws_m = wb.add_worksheet("Monitoring")
    headers_m = [
        "project",
        "file",
        "metric_name",
        "enabled",
        "metricType",
        "schedule_interval",
        "search_interval",
        "search_unit",
        "telegram",
        "mail",
        "team",
    ]
    for col, h in enumerate(headers_m):
        ws_m.write(0, col, h)

    row = 1
    for p in data:
        prefix = p.get("gitlab_group_name", "") or ""
        team = prefix.replace("*", "") if prefix else ""
        for m in p.get("metrics", []):
            rec = dict(m)
            rec["team"] = team
            for col, key in enumerate(headers_m):
                ws_m.write(row, col, str(rec.get(key, "")))
            row += 1

    ws_t = wb.add_worksheet("Teams")
    headers_t = [
        "team",
        "project_count",
        "metrics_total",
        "metrics_active",
        "metrics_disabled",
    ]
    for col, h in enumerate(headers_t):
        ws_t.write(0, col, h)

    row = 1
    for team in sorted(team_stats.keys()):
        st = team_stats[team]
        ws_t.write(row, 0, team)
        ws_t.write(row, 1, len(st["projects"]))
        ws_t.write(row, 2, st["total"])
        ws_t.write(row, 3, st["active"])
        ws_t.write(row, 4, st["disabled"])
        row += 1

    ws_s = wb.add_worksheet("Summary")
    ws_s.write(0, 0, "Metric")
    ws_s.write(0, 1, "Value")

    ws_s.write(1, 0, "Unique users total")
    ws_s.write(1, 1, len(uniq))

    ws_s.write(2, 0, "BK matched users")
    ws_s.write(2, 1, len(matched))

    ws_s.write(3, 0, "Teams total")
    ws_s.write(3, 1, len(team_stats))

    wb.close()
    logger.info("Excel report saved: zeus_report.xlsx")
