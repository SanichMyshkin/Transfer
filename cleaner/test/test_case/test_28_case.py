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


# ======== TEST CASE 28: REPOSITORY ========
def test_repository_case28():
    components = [
        # MATCH
        make_component(
            "compA", "dev-1", last_modified_days_ago=3
        ),  # age<retention → сохраняем
        make_component(
            "compA", "dev-2", last_modified_days_ago=10
        ),  # age>retention → удаляется
        # NO-MATCH
        make_component(
            "compB", "random-1", last_modified_days_ago=2
        ),  # age<no_match_retention → сохраняем
        make_component(
            "compB", "random-2", last_modified_days_ago=10
        ),  # age>no_match_retention → удаляется
    ]

    regex_rules = {"^dev-": {"retention_days": 7}}  # MATCH без reserved

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=7,
        no_match_reserved=None,
        no_match_min_days_since_last_download=None,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"dev-1", "random-1"}
    assert set(deleted_versions) == {"dev-2", "random-2"}


# ======== TEST CASE 28: MAVEN ========
def test_maven_case28():
    now = datetime.now(timezone.utc)
    components = [
        # MATCH
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.0",
            "assets": [{"lastModified": (now - timedelta(days=3)).isoformat()}],
        },  # age<retention → сохраняем
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.1",
            "assets": [{"lastModified": (now - timedelta(days=10)).isoformat()}],
        },  # age>retention → удаляется
        # NO-MATCH
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.0",
            "assets": [{"lastModified": (now - timedelta(days=2)).isoformat()}],
        },  # age<no_match_retention → сохраняем
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.1",
            "assets": [{"lastModified": (now - timedelta(days=10)).isoformat()}],
        },  # age>no_match_retention → удаляется
    ]

    maven_rules = {
        "release": {
            "regex_rules": {"^1.*": {"retention_days": 7}},  # MATCH без reserved
            "no_match_retention_days": 7,
            "no_match_reserved": None,
            "no_match_min_days_since_last_download": None,
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)
    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"1.0.0", "2.0.0"}
    assert set(deleted_versions) == {"1.0.1", "2.0.1"}
