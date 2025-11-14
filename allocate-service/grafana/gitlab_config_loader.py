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

        if not all([self.url, self.token, self.project_id, self.file_path]):
            raise RuntimeError(
                "Missing env vars: GITLAB_URL, GITLAB_TOKEN, GITLAB_PROJECT_ID, GITLAB_FILE_PATH"
            )

        self.gl = gitlab.Gitlab(self.url, private_token=self.token)
        self.project = self.gl.projects.get(self.project_id)

    def load_raw_text(self, ref="main"):
        f = self.project.files.get(file_path=self.file_path, ref=ref)
        b = base64.b64decode(f.content)
        return b.decode("utf-8")

    def load_group_mappings(self, ref="main"):
        raw_text = self.load_raw_text(ref)
        data = tomllib.loads(raw_text)
        mappings = data.get("servers", {}).get("group_mappings", [])

        result = []
        for m in mappings:
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
    mapping = loader.load_group_mappings()
    print(mapping)
