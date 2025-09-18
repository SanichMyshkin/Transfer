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

# ======== TEST CASE 15: REPOSITORY ========
def test_repository_case15():
    components = [
        make_component("compA", "dev-1", last_modified_days_ago=1),   # reserved
        make_component("compA", "dev-2", last_modified_days_ago=2),   # reserved
        make_component("compA", "dev-3", last_modified_days_ago=5),   # age=5 < 7 → сохраняем
        make_component("compA", "dev-4", last_modified_days_ago=12),  # age=12 > 7 → удаляем
    ]

    regex_rules = {"^dev-": {"retention_days": 7, "reserved": 2}}

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=None,   # ❌
        no_match_reserved=None,    # ❌
        no_match_min_days_since_last_download=3,  # ✅
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"dev-1", "dev-2", "dev-3"}
    assert set(deleted_versions) == {"dev-4"}


# ======== TEST CASE 15: MAVEN ========
def test_maven_case15():
    components = [
        {"group": "org.test", "name": "libA", "version": "1.0.0",
         "assets": [{"lastModified": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()}]},
        {"group": "org.test", "name": "libA", "version": "1.0.1",
         "assets": [{"lastModified": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()}]},
        {"group": "org.test", "name": "libA", "version": "1.0.2",
         "assets": [{"lastModified": (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()}]},
        {"group": "org.test", "name": "libA", "version": "1.0.3",
         "assets": [{"lastModified": (datetime.now(timezone.utc) - timedelta(days=12)).isoformat()}]},
    ]

    maven_rules = {
        "release": {
            "regex_rules": {"^1\\.0\\..*": {"retention_days": 7, "reserved": 2}},
            "no_match_retention_days": None,   # ❌
            "no_match_reserved": None,         # ❌
            "no_match_min_days_since_last_download": 3,  # ✅
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"1.0.0", "1.0.1", "1.0.2"}
    assert set(deleted_versions) == {"1.0.3"}
