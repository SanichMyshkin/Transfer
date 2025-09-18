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


# ======== TEST CASE 39: REPOSITORY ========
def test_repository_case39():
    components = [
        # MATCH
        make_component(
            "compA", "dev-1", last_modified_days_ago=3, last_download_days_ago=1
        ),  # reserved → сохраняем
        make_component(
            "compA", "dev-2", last_modified_days_ago=5, last_download_days_ago=2
        ),  # dl < min_days → сохраняем
        make_component(
            "compA", "dev-3", last_modified_days_ago=6, last_download_days_ago=20
        ),  # dl > min_days → удаляется
        make_component(
            "compA", "dev-4", last_modified_days_ago=7, last_download_days_ago=None
        ),  # нет скачивания → удаляется
        # NO-MATCH
        make_component(
            "compB", "random-1", last_modified_days_ago=1, last_download_days_ago=2
        ),  # dl < no_match_min_days → сохраняем
        make_component(
            "compB", "random-2", last_modified_days_ago=2, last_download_days_ago=15
        ),  # dl > no_match_min_days → удаляется
        make_component(
            "compB", "random-3", last_modified_days_ago=3
        ),  # нет скачивания → удаляется
    ]

    regex_rules = {"^dev-": {"reserved": 1, "min_days_since_last_download": 7}}

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=None,
        no_match_reserved=None,
        no_match_min_days_since_last_download=7,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"dev-1", "dev-2", "random-1"}
    assert set(deleted_versions) == {"dev-3", "dev-4", "random-2", "random-3"}


# ======== TEST CASE 39: MAVEN ========
def test_maven_case39():
    now = datetime.now(timezone.utc)
    components = [
        # MATCH
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.0",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=3)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=1)).isoformat(),
                }
            ],
        },  # reserved → сохраняем
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.1",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=5)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=2)).isoformat(),
                }
            ],
        },  # dl < min_days → сохраняем
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.2",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=6)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=20)).isoformat(),
                }
            ],
        },  # dl > min_days → удаляется
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.3",
            "assets": [{"lastModified": (now - timedelta(days=7)).isoformat()}],
        },  # нет скачивания → удаляется
        # NO-MATCH
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.0",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=1)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=2)).isoformat(),
                }
            ],
        },  # dl < no_match_min_days → сохраняем
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.1",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=2)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=15)).isoformat(),
                }
            ],
        },  # dl > no_match_min_days → удаляется
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.2",
            "assets": [{"lastModified": (now - timedelta(days=3)).isoformat()}],
        },  # нет скачивания → удаляется
    ]

    maven_rules = {
        "release": {
            "regex_rules": {"^1.*": {"reserved": 1, "min_days_since_last_download": 7}},
            "no_match_retention_days": None,
            "no_match_reserved": None,
            "no_match_min_days_since_last_download": 7,
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)
    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"1.0.0", "1.0.1", "2.0.0"}
    assert set(deleted_versions) == {"1.0.2", "1.0.3", "2.0.1", "2.0.2"}
