# excel_report.py

import pandas as pd
import xlsxwriter
from config import REPORT_PATH


def build_excel_report(
    repo_sizes, repo_data, role_repo_map, ad_map, log_stats, output_file=REPORT_PATH
):
    """
    repo_sizes: dict {repo: size_bytes}
    repo_data: list[{...}]
    role_repo_map: dict {role_id: [repo1, repo2]}
    ad_map: dict {role_id: AD_group}
    log_stats: dict из log_filter.process_logs()
    """

    df_repo_sizes = pd.DataFrame(
        [{"repository": r, "size_bytes": size} for r, size in repo_sizes.items()]
    )

    df_repo_data = pd.DataFrame(repo_data)

    df_roles = pd.DataFrame(
        [
            {
                "role_id": role,
                "ad_group": ad_map.get(role, ""),
                "repositories": ", ".join(repos),
            }
            for role, repos in role_repo_map.items()
        ]
    )

    df_repo_stats = log_stats["repo_stats"]
    df_users_by_repo = log_stats["users_by_repo"]
    df_normal = log_stats["normal_users"]
    df_anonymous = log_stats["anonymous_users"]

    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        df_repo_sizes.to_excel(writer, sheet_name="Repo Sizes", index=False)
        df_repo_data.to_excel(writer, sheet_name="Repositories", index=False)
        df_roles.to_excel(writer, sheet_name="Roles → AD → Repos", index=False)

        df_repo_stats.to_excel(writer, sheet_name="Repo Stats", index=False)
        df_users_by_repo.to_excel(writer, sheet_name="Users by Repo", index=False)

        df_normal.to_excel(writer, sheet_name="Normal Users", index=False)
        df_anonymous.to_excel(writer, sheet_name="Anonymous Users", index=False)

        # автоподгон ширины
        for sheet_name, df in {
            "Repo Sizes": df_repo_sizes,
            "Repositories": df_repo_data,
            "Roles → AD → Repos": df_roles,
            "Repo Stats": df_repo_stats,
            "Users by Repo": df_users_by_repo,
            "Normal Users": df_normal,
            "Anonymous Users": df_anonymous,
        }.items():
            worksheet = writer.sheets[sheet_name]
            for i, col in enumerate(df.columns):
                width = max(df[col].astype(str).map(len).max(), len(col)) + 2

                worksheet.set_column(i, i, width)

    print(f"📊 Отчёт создан: {output_file}")
