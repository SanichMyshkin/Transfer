import os
import gitlab
import tomllib


class GitLabConfigLoader:
    def __init__(self):
        self.url = os.getenv("GITLAB_URL")
        self.token = os.getenv("GITLAB_TOKEN")
        self.project_id = int(os.getenv("GITLAB_PROJECT_ID", "3058"))
        self.file_path = os.getenv("GITLAB_FILE_PATH", "grafana_main/ldap.toml")
        self.gl = gitlab.Gitlab(self.url, private_token=self.token, ssl_verify=False)

    def load_file(self):
        project = self.gl.projects.get(self.project_id)
        f = project.files.get(file_path=self.file_path, ref="main")
        content = f.decode().decode("utf-8")
        return content

    def load_group_mappings(self):
        raw = self.load_file()
        data = tomllib.loads(raw)

        if "servers" not in data:
            return {}

        servers_raw = data["servers"]
        if isinstance(servers_raw, dict):
            servers = [servers_raw]
        else:
            servers = servers_raw

        mappings = {}
        for srv in servers:
            arr = srv.get("group_mappings", [])
            for item in arr:
                org = item.get("org_id")
                if not org:
                    continue
                mappings.setdefault(org, []).append({
                    "org_id": org,
                    "group_dn": item.get("group_dn"),
                    "org_role": item.get("org_role"),
                    "grafana_admin": item.get("grafana_admin", False)
                })
        return mappings

    def get_owners_clean(self):
        m = self.load_group_mappings()
        cleaned = {}

        for org, items in m.items():
            admins = [i for i in items if i.get("org_role") == "Admin"]
            if not admins:
                continue

            non_global = [i for i in admins if not i.get("grafana_admin")]
            if non_global:
                g = non_global[0]["group_dn"].split(",")[0].replace("CN=", "")
                cleaned[org] = (g, False)
            else:
                g = admins[0]["group_dn"].split(",")[0].replace("CN=", "")
                cleaned[org] = (g, True)

        return cleaned
