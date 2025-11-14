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
            raise RuntimeError("Missing required env vars: GITLAB_URL, GITLAB_TOKEN")

        self.gl = gitlab.Gitlab(
            self.url,
            private_token=self.token,
            ssl_verify=False,
        )

        self.project = self.gl.projects.get(self.project_id)

    def load_raw_text(self) -> str:
        f = self.project.files.get(file_path=self.file_path, ref=self.ref)
        decoded = base64.b64decode(f.content)
        return decoded.decode("utf-8")

    def load_group_mappings(self):
        text = self.load_raw_text()
        data = tomllib.loads(text)

        servers = data.get("servers")
        mappings_raw = []

        if isinstance(servers, dict):
            gm = servers.get("group_mappings", [])
            if isinstance(gm, list):
                mappings_raw.extend(gm)

        elif isinstance(servers, list):
            for item in servers:
                if isinstance(item, dict):
                    gm = item.get("group_mappings")
                    if isinstance(gm, list):
                        mappings_raw.extend(gm)

        result = []
        for m in mappings_raw:
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
                    "grafana_admin": bool(m.get("grafana_admin", False)),
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
            internal_admins = [
                g
                for g in groups
                if g.get("org_role") == "Admin" and g.get("grafana_admin") is False
            ]
            if internal_admins:
                owners[org_id] = internal_admins
                continue

            global_admins = [
                g
                for g in groups
                if g.get("org_role") == "Admin" and g.get("grafana_admin") is True
            ]
            if global_admins:
                owners[org_id] = global_admins
            else:
                owners[org_id] = []

        return owners

    def get_owners_clean(self):
        owners = self.detect_org_owners()
        cleaned = {}

        for org_id, groups in owners.items():
            unique = set()

            for g in groups:
                dn = g.get("group_dn") or ""
                first_part = dn.split(",")[0].strip()
                if first_part.upper().startswith("CN="):
                    cn = first_part[3:].strip()
                else:
                    cn = first_part

                unique.add((cn, bool(g.get("grafana_admin"))))

            if len(unique) == 0:
                cleaned[org_id] = []
            elif len(unique) == 1:
                cleaned[org_id] = next(iter(unique))
            else:
                cleaned[org_id] = sorted(unique)

        return cleaned
