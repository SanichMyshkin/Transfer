from datetime import datetime, timedelta, timezone

from repository import filter_components_to_delete
from maven import filter_maven_components_to_delete


def make_component(name, version, last_modified_days_ago, last_download_days_ago=None):
    now = datetime.now(timezone.utc)
    last_modified = now - timedelta(days=last_modified_days_ago)
    asset = {"lastModified": last_modified.isoformat()}
    if last_download_days_ago is not None:
        last_download = now - timedelta(days=last_download_days_ago)
        asset["lastDownloaded"] = last_download.isoformat()
    return {"name": name, "version": version, "assets": [asset]}


# ======== TEST CASE 62: REPOSITORY ========
def test_repository_case62():
    components = [
        make_component("compX", "v1", last_modified_days_ago=1),   # top reserved → сохраняем
        make_component("compX", "v2", last_modified_days_ago=5),   # удаляется (нет retention/min_days)
        make_component("compX", "v3", last_modified_days_ago=10),  # удаляется
    ]

    regex_rules = {}  # нет правил match

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=None,
        no_match_reserved=1,
        no_match_min_days_since_last_download=None,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"v1"}
    assert set(deleted_versions) == {"v2", "v3"}


# ======== TEST CASE 62: MAVEN ========
def test_maven_case62():
    now = datetime.now(timezone.utc)
    components = [
        {
            "group": "org.demo",
            "name": "libX",
            "version": "1.0.0",
            "assets": [{"lastModified": (now - timedelta(days=1)).isoformat()}],
        },  # top reserved → сохраняем
        {
            "group": "org.demo",
            "name": "libX",
            "version": "1.0.1",
            "assets": [{"lastModified": (now - timedelta(days=5)).isoformat()}],
        },  # удаляется
        {
            "group": "org.demo",
            "name": "libX",
            "version": "1.0.2",
            "assets": [{"lastModified": (now - timedelta(days=10)).isoformat()}],
        },  # удаляется
    ]

    maven_rules = {
        "release": {
            "regex_rules": {},  # нет правил match
            "no_match_retention_days": None,
            "no_match_reserved": 1,
            "no_match_min_days_since_last_download": None,
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)
    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"1.0.0"}
    assert set(deleted_versions) == {"1.0.1", "1.0.2"}
