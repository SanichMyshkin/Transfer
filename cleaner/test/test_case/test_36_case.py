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


# ======== TEST CASE 36: REPOSITORY ========
def test_repository_case36():
    components = [
        # MATCH (reserved=1, retention=7, min_days=5)
        make_component(
            "compA", "dev-1", last_modified_days_ago=20, last_download_days_ago=20
        ),
        make_component(
            "compA", "dev-2", last_modified_days_ago=3, last_download_days_ago=20
        ),  # reserved (свежий)
        make_component(
            "compA", "dev-3", last_modified_days_ago=20, last_download_days_ago=2
        ),  # dl<5 → сохраняем
        make_component(
            "compA", "dev-4", last_modified_days_ago=20, last_download_days_ago=20
        ),  # удаляется
        # NO-MATCH (retention=7, без reserved и min_days)
        make_component(
            "compB", "random-1", last_modified_days_ago=20, last_download_days_ago=20
        ),  # age>7 → удаляем
        make_component(
            "compB", "random-2", last_modified_days_ago=3, last_download_days_ago=20
        ),  # age<7 → сохраняем
        make_component(
            "compB", "random-3", last_modified_days_ago=20, last_download_days_ago=2
        ),  # age>7 → удаляем
        make_component(
            "compB", "random-4", last_modified_days_ago=20, last_download_days_ago=20
        ),  # age>7 → удаляем
    ]

    regex_rules = {
        "^dev-": {"retention_days": 7, "reserved": 1, "min_days_since_last_download": 5}
    }

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=7,
        no_match_reserved=None,
        no_match_min_days_since_last_download=None,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # MATCH: dev-2 (reserved), dev-3 (dl<5)
    # NO-MATCH: random-2 (age<7)
    assert set(saved_versions) == {"dev-2", "dev-3", "random-2"}
    assert set(deleted_versions) == {
        "dev-1",
        "dev-4",
        "random-1",
        "random-3",
        "random-4",
    }


# ======== TEST CASE 36: MAVEN ========
def test_maven_case36():
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
        },  # reserved (свежий)
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
        },  # dl<5 → сохраняем
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
        },  # удаляется
        # NO-MATCH (retention=7, без reserved и min_days)
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
        },  # age>7 → удаляем
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.1",
            "assets": [{"lastModified": (now - timedelta(days=3)).isoformat()}],
        },  # age<7 → сохраняем
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
        },  # age>7 → удаляем
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
        },  # age>7 → удаляем
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
            "no_match_reserved": None,
            "no_match_min_days_since_last_download": None,
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # MATCH: 1.0.1 (reserved), 1.0.2 (dl<5)
    # NO-MATCH: 2.0.1 (age<7)
    assert set(saved_versions) == {"1.0.1", "1.0.2", "2.0.1"}
    assert set(deleted_versions) == {"1.0.0", "1.0.3", "2.0.0", "2.0.2", "2.0.3"}
