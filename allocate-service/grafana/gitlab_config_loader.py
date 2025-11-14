import os
import base64
import gitlab

# tomllib for Python 3.11+, otherwise fallback to tomli
try:
    import tomllib
except ImportError:
    import tomli as tomllib


class GitLabConfigLoader:
    def __init__(self):
        self.url = os.getenv("GITLAB_URL")
        self.token = os.getenv("GITLAB_TOKEN")

        # Defaults as requested
        self.project_id = os.getenv("GITLAB_PROJECT_ID", "3058")
        self.file_path = os.getenv("GITLAB_FILE_PATH", "grafana_main/ldap.toml")
        self.ref = os.getenv("GITLAB_REF", "main")

        if not all([self.url, self.token]):
            raise RuntimeError(
                "Missing required vars: GITLAB_URL, GITLAB_TOKEN"
            )

        # Disable SSL verification
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

        # case: servers = dict
        if isinstance(servers, dict):
            gm = servers.get("group_mappings", [])
            if isinstance(gm, list):
                mappings.extend(gm)

        # case: servers = list (array-of-tables)
        elif isinstance(servers, list):
            for item in servers:
                if isinstance(item, dict) and "group_mappings" in item:
                    gm = item["group_mappings"]
                    if isinstance(gm, list):
                        mappings.extend(gm)

        result = []
        for m in mappings:
            if not isinstance(m, dict):
                continue

            org_id = m.get("org_id")
            if org_id is None:
                continue

            result.append(
                {
                    "org_id": org_id,
                    "group_dn": m.get("group_dn"),
                    "org_role": m.get("org_role"),
                    "grafana_admin": m.get("grafana_admin", False),
                }
            )

        return result


if __name__ == "__main__":
    loader = GitLabConfigLoader()
    mappings = loader.load_group_mappings()
    print(mappings)
