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


def make_maven_component(group, name, version, last_modified_days_ago, last_download_days_ago=None):
    now = datetime.now(timezone.utc)
    last_modified = now - timedelta(days=last_modified_days_ago)
    asset = {"lastModified": last_modified.isoformat()}
    if last_download_days_ago is not None:
        last_download = now - timedelta(days=last_download_days_ago)
        asset["lastDownloaded"] = last_download.isoformat()
    return {"group": group, "name": name, "version": version, "assets": [asset]}


# ======== TEST CASE 11: MATCH (repository) ========
def test_repository_case11_match():
    components = [
        make_component("compA", "dev-1", last_modified_days_ago=1),   # reserved
        make_component("compA", "dev-2", last_modified_days_ago=2),   # reserved
        make_component("compA", "dev-3", last_modified_days_ago=6),   # age=6 <= retention=7 → сохраняем
        make_component("compA", "dev-4", last_modified_days_ago=12),  # age=12 > retention=7 → удаляем
    ]

    regex_rules = {"^dev-": {"retention_days": 7, "reserved": 2}}

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=5,
        no_match_reserved=None,
        no_match_min_days_since_last_download=3,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"dev-1", "dev-2", "dev-3"}
    assert set(deleted_versions) == {"dev-4"}


# ======== TEST CASE 11: NO-MATCH (repository) ========
def test_repository_case11_no_match():
    components = [
        make_component("compB", "random-1", last_modified_days_ago=2, last_download_days_ago=1),   # age=2 <= 5 → сохраняем
        make_component("compB", "random-2", last_modified_days_ago=8, last_download_days_ago=2),   # age=8 > 5, но dl=2 <= 3 → сохраняем
        make_component("compB", "random-3", last_modified_days_ago=10, last_download_days_ago=15), # age=10 > 5, dl=15 > 3 → удаляем
    ]

    regex_rules = {"^dev-": {"retention_days": 7, "reserved": 2}}  # не подходит

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=5,
        no_match_reserved=None,
        no_match_min_days_since_last_download=3,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"random-1", "random-2"}
    assert set(deleted_versions) == {"random-3"}


# ======== TEST CASE 11: MATCH (maven) ========
def test_maven_case11_match():
    components = [
        make_maven_component("org.test", "libA", "1.0.0", last_modified_days_ago=1),   # reserved
        make_maven_component("org.test", "libA", "1.0.1", last_modified_days_ago=2),   # reserved
        make_maven_component("org.test", "libA", "1.0.2", last_modified_days_ago=6),   # age=6 <= retention=7 → сохраняем
        make_maven_component("org.test", "libA", "1.0.3", last_modified_days_ago=12),  # age=12 > retention=7 → удаляем
    ]

    maven_rules = {
        "release": {
            "regex_rules": {"^1\\.0\\.": {"retention_days": 7, "reserved": 2}},
            "no_match_retention_days": 5,
            "no_match_reserved": None,
            "no_match_min_days_since_last_download": 3,
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"1.0.0", "1.0.1", "1.0.2"}
    assert set(deleted_versions) == {"1.0.3"}


# ======== TEST CASE 11: NO-MATCH (maven) ========
def test_maven_case11_no_match():
    components = [
        make_maven_component("org.test", "libB", "2.0.0", last_modified_days_ago=2, last_download_days_ago=1),   # age=2 <= 5 → сохраняем
        make_maven_component("org.test", "libB", "2.0.1", last_modified_days_ago=8, last_download_days_ago=2),   # age=8 > 5, но dl=2 <= 3 → сохраняем
        make_maven_component("org.test", "libB", "2.0.2", last_modified_days_ago=10, last_download_days_ago=15), # age=10 > 5, dl=15 > 3 → удаляем
    ]

    maven_rules = {
        "release": {
            "regex_rules": {"^1\\.0\\.": {"retention_days": 7, "reserved": 2}},  # не подходит
            "no_match_retention_days": 5,
            "no_match_reserved": None,
            "no_match_min_days_since_last_download": 3,
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"2.0.0", "2.0.1"}
    assert set(deleted_versions) == {"2.0.2"}
