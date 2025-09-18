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


# ======== TEST CASE 63: REPOSITORY ========
def test_repository_case63():
    components = [
        make_component(
            "compY", "v1", last_modified_days_ago=10, last_download_days_ago=2
        ),  # dl=2 ≤ min_days=5 → сохраняем
        make_component(
            "compY", "v2", last_modified_days_ago=10, last_download_days_ago=7
        ),  # dl=7 > min_days=5 → удаляется
        make_component(
            "compY", "v3", last_modified_days_ago=10, last_download_days_ago=None
        ),  # нет скачивания → удаляется
    ]

    regex_rules = {}  # нет правил match

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=None,
        no_match_reserved=None,
        no_match_min_days_since_last_download=5,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"v1"}
    assert set(deleted_versions) == {"v2", "v3"}


# ======== TEST CASE 63: MAVEN ========
def test_maven_case63():
    now = datetime.now(timezone.utc)
    components = [
        {
            "group": "org.demo",
            "name": "libY",
            "version": "1.0.0",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=10)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=2)).isoformat(),
                }
            ],
        },  # dl=2 ≤ min_days=5 → сохраняем
        {
            "group": "org.demo",
            "name": "libY",
            "version": "1.0.1",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=10)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=7)).isoformat(),
                }
            ],
        },  # dl=7 > min_days=5 → удаляется
        {
            "group": "org.demo",
            "name": "libY",
            "version": "1.0.2",
            "assets": [{"lastModified": (now - timedelta(days=10)).isoformat()}],
        },  # нет скачивания → удаляется
    ]

    maven_rules = {
        "release": {
            "regex_rules": {},  # нет правил match
            "no_match_retention_days": None,
            "no_match_reserved": None,
            "no_match_min_days_since_last_download": 5,
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)
    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"1.0.0"}
    assert set(deleted_versions) == {"1.0.1", "1.0.2"}
