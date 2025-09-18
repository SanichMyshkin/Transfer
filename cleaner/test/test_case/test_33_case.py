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


# ======== TEST CASE 33: REPOSITORY ========
def test_repository_case33():
    components = [
        # MATCH (reserved=1, retention=7, min_days=5)
        make_component("compA", "dev-1", last_modified_days_ago=20, last_download_days_ago=20),
        make_component("compA", "dev-2", last_modified_days_ago=3, last_download_days_ago=20),
        make_component("compA", "dev-3", last_modified_days_ago=20, last_download_days_ago=2),
        make_component("compA", "dev-4", last_modified_days_ago=20, last_download_days_ago=20),

        # NO-MATCH (reserved=1, retention=7, min_days=5)
        make_component("compB", "random-1", last_modified_days_ago=20, last_download_days_ago=20),
        make_component("compB", "random-2", last_modified_days_ago=3, last_download_days_ago=20),
        make_component("compB", "random-3", last_modified_days_ago=20, last_download_days_ago=2),
        make_component("compB", "random-4", last_modified_days_ago=20, last_download_days_ago=20),
    ]

    regex_rules = {
        "^dev-": {"retention_days": 7, "reserved": 1, "min_days_since_last_download": 5}
    }

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=7,
        no_match_reserved=1,
        no_match_min_days_since_last_download=5,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # reserved = самый свежий → dev-2 и random-2
    # дополнительно сохраняем dev-3 и random-3 (так как dl < min_days)
    assert set(saved_versions) == {"dev-2", "dev-3", "random-2", "random-3"}
    assert set(deleted_versions) == {"dev-1", "dev-4", "random-1", "random-4"}


# ======== TEST CASE 33: MAVEN ========
def test_maven_case33():
    now = datetime.now(timezone.utc)
    components = [
        # MATCH (reserved=1, retention=7, min_days=5)
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.0",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=20)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=20)).isoformat(),
                }
            ],
        },
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.1",
            "assets": [{"lastModified": (now - timedelta(days=3)).isoformat()}],
        },
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.2",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=20)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=2)).isoformat(),
                }
            ],
        },
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.3",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=20)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=20)).isoformat(),
                }
            ],
        },

        # NO-MATCH (reserved=1, retention=7, min_days=5)
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.0",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=20)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=20)).isoformat(),
                }
            ],
        },
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.1",
            "assets": [{"lastModified": (now - timedelta(days=3)).isoformat()}],
        },
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.2",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=20)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=2)).isoformat(),
                }
            ],
        },
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.3",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=20)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=20)).isoformat(),
                }
            ],
        },
    ]

    maven_rules = {
        "release": {
            "regex_rules": {
                "^1.*": {
                    "retention_days": 7,
                    "reserved": 1,
                    "min_days_since_last_download": 5,
                }
            },
            "no_match_retention_days": 7,
            "no_match_reserved": 1,
            "no_match_min_days_since_last_download": 5,
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # reserved = самые свежие → 1.0.1 и 2.0.1
    # дополнительно сохраняем 1.0.2 и 2.0.2 (так как dl < min_days)
    assert set(saved_versions) == {"1.0.1", "1.0.2", "2.0.1", "2.0.2"}
    assert set(deleted_versions) == {"1.0.0", "1.0.3", "2.0.0", "2.0.3"}
