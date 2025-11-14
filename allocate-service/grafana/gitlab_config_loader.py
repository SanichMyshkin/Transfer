import os
import base64
import gitlab

try:
    import tomllib
except ImportError:
    import tomli as tomllib


class GitLabConfigLoader:
    def __init__(self):
        self.url = os.getenv("GITLAB_URL")
        self.token = os.getenv("GITLAB_TOKEN")

        self.project_id = os.getenv("GITLAB_PROJECT_ID", "3058")
        self.file_path = os.getenv("GITLAB_FILE_PATH", "grafana_main/ldap.toml")
        self.ref = os.getenv("GITLAB_REF", "main")

        if not all([self.url, self.token]):
            raise RuntimeError("Missing required: GITLAB_URL, GITLAB_TOKEN")

        self.gl = gitlab.Gitlab(
            self.url,
            private_token=self.token,
            ssl_verify=False
        )

        self.project = self.gl.projects.get(self.project_id)

    def load_raw_text(self):
        f = self.project.files.get(file_path=self.file_path, ref=self.ref)
        decoded = base64.b64decode(f.content)
        return decoded.decode("utf-8")

    def load_group_mappings(self):
        text = self.load_raw_text()
        data = tomllib.loads(text)

        servers = data.get("servers")
        mappings = []

        # Case 1 — servers: dict
        if isinstance(servers, dict):
            gm = servers.get("group_mappings", [])
            if isinstance(gm, list):
                mappings.extend(gm)

        # Case 2 — servers: list
        elif isinstance(servers, list):
            for item in servers:
                if isinstance(item, dict) and "group_mappings" in item:
                    gm = item["group_mappings"]
                    if isinstance(gm, list):
                        mappings.extend(gm)

        # Filter out invalid entries
        result = []
        for m in mappings:
            if not isinstance(m, dict):
                continue
            if m.get("org_id") is None:
                continue
            result.append(
                {
                    "org_id": m.get("org_id"),
                    "group_dn": m.get("group_dn"),
                    "org_role": m.get("org_role"),
                    "grafana_admin": m.get("grafana_admin", False),
                }
            )

        return result

    def detect_org_owners(self):
        mappings = self.load_group_mappings()
        orgs = {}

        for m in mappings:
            org_id = m["org_id"]
            orgs.setdefault(org_id, []).append(m)

        owners = {}

        for org_id, groups in orgs.items():

            # 1) Локальные владельцы (основные)
            internal_admins = [
                g for g in groups
                if g.get("org_role") == "Admin" and g.get("grafana_admin") is False
            ]

            if internal_admins:
                owners[org_id] = internal_admins
                continue

            # 2) Если локальных нет — берем глобальных (fallback)
            global_admins = [
                g for g in groups
                if g.get("org_role") == "Admin" and g.get("grafana_admin") is True
            ]

            if global_admins:
                owners[org_id] = global_admins
                continue

            # 3) Если админов нет — владельцев нет
            owners[org_id] = []

        return owners



if __name__ == "__main__":
    loader = GitLabConfigLoader()
    owners = loader.detect_org_owners()
    print(owners)
