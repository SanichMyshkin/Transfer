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


# ======== TEST CASE 41: REPOSITORY ========
def test_repository_case41():
    components = [
        # MATCH
        make_component(
            "compA", "dev-1", last_modified_days_ago=1, last_download_days_ago=1
        ),  # reserved → сохраняем
        make_component(
            "compA", "dev-2", last_modified_days_ago=2, last_download_days_ago=2
        ),  # удаляется (нет retention/min_days)
        make_component(
            "compA", "dev-3", last_modified_days_ago=3, last_download_days_ago=None
        ),  # удаляется
        # NO-MATCH
        make_component(
            "compB", "random-1", last_modified_days_ago=1, last_download_days_ago=1
        ),  # reserved → сохраняем
        make_component(
            "compB", "random-2", last_modified_days_ago=5, last_download_days_ago=2
        ),  # dl < no_match_min_days → сохраняем
        make_component(
            "compB", "random-3", last_modified_days_ago=10, last_download_days_ago=20
        ),  # age < no_match_retention? нет, dl < min_days? нет → удаляется
        make_component(
            "compB", "random-4", last_modified_days_ago=3, last_download_days_ago=None
        ),  # age < retention → сохраняем
    ]

    regex_rules = {"^dev-": {"reserved": 1}}  # только reserved

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=7,
        no_match_reserved=1,
        no_match_min_days_since_last_download=7,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"dev-1", "random-1", "random-2", "random-4"}
    assert set(deleted_versions) == {"dev-2", "dev-3", "random-3"}


# ======== TEST CASE 41: MAVEN ========
def test_maven_case41():
    now = datetime.now(timezone.utc)
    components = [
        # MATCH
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.0",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=1)).isoformat(),
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
                    "lastModified": (now - timedelta(days=2)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=2)).isoformat(),
                }
            ],
        },  # удаляется
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.2",
            "assets": [{"lastModified": (now - timedelta(days=3)).isoformat()}],
        },  # удаляется
        # NO-MATCH
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.0",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=1)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=1)).isoformat(),
                }
            ],
        },  # reserved → сохраняем
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.1",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=5)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=2)).isoformat(),
                }
            ],
        },  # dl < min_days → сохраняем
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.2",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=10)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=20)).isoformat(),
                }
            ],
        },  # age > retention и dl > min_days → удаляется
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.3",
            "assets": [{"lastModified": (now - timedelta(days=3)).isoformat()}],
        },  # age < retention → сохраняем
    ]

    maven_rules = {
        "release": {
            "regex_rules": {"^1.*": {"reserved": 1}},  # только reserved
            "no_match_retention_days": 7,
            "no_match_reserved": 1,
            "no_match_min_days_since_last_download": 7,
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"1.0.0", "2.0.0", "2.0.1", "2.0.3"}
    assert set(deleted_versions) == {"1.0.1", "1.0.2", "2.0.2"}
