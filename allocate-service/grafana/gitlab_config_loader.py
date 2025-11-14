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
        self.project_id = os.getenv("GITLAB_PROJECT_ID")
        self.file_path = os.getenv("GITLAB_FILE_PATH")
        self.ref = os.getenv("GITLAB_REF", "main")

        if not all([self.url, self.token, self.project_id, self.file_path]):
            raise RuntimeError("Missing GitLab env vars")

        self.gl = gitlab.Gitlab(self.url, private_token=self.token)
        self.project = self.gl.projects.get(self.project_id)

    def load_raw_text(self):
        f = self.project.files.get(file_path=self.file_path, ref=self.ref)
        decoded = base64.b64decode(f.content)
        return decoded.decode("utf-8")

    def load_group_mappings(self):
        text = self.load_raw_text()
        data = tomllib.loads(text)

        servers = data.get("servers")
        if not isinstance(servers, dict):
            raise RuntimeError("TOML format error: [servers] must be a table")

        mappings = servers.get("group_mappings")
        if not isinstance(mappings, list):
            raise RuntimeError("TOML format error: [[servers.group_mappings]] must be a list")

        result = []
        for m in mappings:
            if not isinstance(m, dict):
                continue

            org_id = m.get("org_id")
            group_dn = m.get("group_dn")
            org_role = m.get("org_role")
            grafana_admin = m.get("grafana_admin", False)

            if org_id is None:
                # просто пропускаем записи без org_id, но можно и логировать
                continue

            result.append({
                "org_id": org_id,
                "group_dn": group_dn,
                "org_role": org_role,
                "grafana_admin": grafana_admin,
            })

        return result


if __name__ == "__main__":
    loader = GitLabConfigLoader()
    mappings = loader.load_group_mappings()
    print(mappings)
