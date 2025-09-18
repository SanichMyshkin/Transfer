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


# ======== TEST CASE 17: REPOSITORY ========
def test_repository_case17():
    components = [
        make_component("compA", "dev-1", last_modified_days_ago=2, last_download_days_ago=1),  # сохранится: age < retention
        make_component("compA", "dev-2", last_modified_days_ago=10, last_download_days_ago=1), # удалится: age > retention
        make_component("compA", "dev-3", last_modified_days_ago=12, last_download_days_ago=5), # удалится: age > retention
    ]

    regex_rules = {"^dev-": {"retention_days": 7}}  # без reserved и min_days

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=5,
        no_match_reserved=1,
        no_match_min_days_since_last_download=3,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # MATCH: dev-1 → сохраняется, dev-2/3 → удаляются (age > retention)
    assert set(saved_versions) == {"dev-1"}
    assert set(deleted_versions) == {"dev-2", "dev-3"}


# ======== TEST CASE 17: MAVEN ========
def test_maven_case17():
    now = datetime.now(timezone.utc)
    components = [
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.0",
            "assets": [
                {"lastModified": (now - timedelta(days=2)).isoformat(),
                 "lastDownloaded": (now - timedelta(days=1)).isoformat()}
            ],
        },
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.1",
            "assets": [
                {"lastModified": (now - timedelta(days=10)).isoformat(),
                 "lastDownloaded": (now - timedelta(days=1)).isoformat()}
            ],
        },
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.2",
            "assets": [
                {"lastModified": (now - timedelta(days=12)).isoformat(),
                 "lastDownloaded": (now - timedelta(days=5)).isoformat()}
            ],
        },
    ]

    maven_rules = {
        "release": {
            "regex_rules": {"^1.*": {"retention_days": 7}},  # без reserved и min_days
            "no_match_retention_days": 5,
            "no_match_reserved": 1,
            "no_match_min_days_since_last_download": 3,
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)
    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # MATCH: 1.0.0 → age=2 < 7 → сохраняем, 1.0.1/1.0.2 → age>7 → удаляются
    assert set(saved_versions) == {"1.0.0"}
    assert set(deleted_versions) == {"1.0.1", "1.0.2"}
