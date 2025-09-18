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


# ======== TEST CASE 64: REPOSITORY ========
def test_repository_case64():
    components = [
        make_component("compZ", "v1", last_modified_days_ago=1),
        make_component("compZ", "v2", last_modified_days_ago=5),
        make_component("compZ", "v3", last_modified_days_ago=10),
    ]

    regex_rules = {}  # нет правил match

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=None,
        no_match_reserved=None,
        no_match_min_days_since_last_download=None,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # Все должны сохраниться
    assert set(saved_versions) == {"v1", "v2", "v3"}
    assert deleted_versions == []


# ======== TEST CASE 64: MAVEN ========
def test_maven_case64():
    now = datetime.now(timezone.utc)
    components = [
        {
            "group": "org.demo",
            "name": "libZ",
            "version": "1.0.0",
            "assets": [{"lastModified": (now - timedelta(days=1)).isoformat()}],
        },
        {
            "group": "org.demo",
            "name": "libZ",
            "version": "1.0.1",
            "assets": [{"lastModified": (now - timedelta(days=5)).isoformat()}],
        },
        {
            "group": "org.demo",
            "name": "libZ",
            "version": "1.0.2",
            "assets": [{"lastModified": (now - timedelta(days=10)).isoformat()}],
        },
    ]

    maven_rules = {
        "release": {
            "regex_rules": {},  # нет правил match
            "no_match_retention_days": None,
            "no_match_reserved": None,
            "no_match_min_days_since_last_download": None,
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)
    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # Все должны сохраниться
    assert set(saved_versions) == {"1.0.0", "1.0.1", "1.0.2"}
    assert deleted_versions == []
