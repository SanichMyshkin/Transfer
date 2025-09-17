import pytest
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


# ======== TEST: repository.py ========
def test_repository_fourth_case():
    components = [
        make_component(
            "compA", "dev-1", last_modified_days_ago=2, last_download_days_ago=1
        ),
        make_component(
            "compA", "dev-2", last_modified_days_ago=6, last_download_days_ago=10
        ),
        make_component(
            "compA", "dev-3", last_modified_days_ago=1, last_download_days_ago=0
        ),
    ]

    regex_rules = {
        "^dev-": {"retention_days": 5, "reserved": 2, "min_days_since_last_download": 3}
    }

    no_match_retention = 10
    no_match_reserved = None  # ❌
    no_match_min_days_since_last_download = None  # ❌

    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention,
        no_match_reserved,
        no_match_min_days_since_last_download,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]

    # Сортировка по last_modified: dev-3 (1 день), dev-1 (2 дня), dev-2 (6 дней)
    # Top-2 reserved → dev-3 и dev-1
    assert set(saved_versions) == {"dev-3", "dev-1"}


# ======== TEST: maven.py ========
def test_maven_fourth_case():
    components = [
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.0",
            "assets": [
                {
                    "lastModified": (
                        datetime.now(timezone.utc) - timedelta(days=2)
                    ).isoformat(),
                    "lastDownloaded": (
                        datetime.now(timezone.utc) - timedelta(days=1)
                    ).isoformat(),
                }
            ],
        },
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.1",
            "assets": [
                {
                    "lastModified": (
                        datetime.now(timezone.utc) - timedelta(days=6)
                    ).isoformat(),
                    "lastDownloaded": (
                        datetime.now(timezone.utc) - timedelta(days=10)
                    ).isoformat(),
                }
            ],
        },
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.2",
            "assets": [
                {
                    "lastModified": (
                        datetime.now(timezone.utc) - timedelta(days=1)
                    ).isoformat(),
                    "lastDownloaded": (
                        datetime.now(timezone.utc) - timedelta(days=0)
                    ).isoformat(),
                }
            ],
        },
    ]

    maven_rules = {
        "release": {
            "regex_rules": {
                ".*": {
                    "retention_days": 5,
                    "reserved": 2,
                    "min_days_since_last_download": 3,
                }
            },
            "no_match_retention_days": 10,
            "no_match_reserved": None,  # ❌
            "no_match_min_days_since_last_download": None,  # ❌
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)

    saved_versions = [c["version"] for c in components if not c["will_delete"]]

    # Сортировка по last_modified: 1.0.2 (1 день), 1.0.0 (2 дня), 1.0.1 (6 дней)
    # Top-2 reserved → 1.0.2 и 1.0.0
    assert set(saved_versions) == {"1.0.2", "1.0.0"}
