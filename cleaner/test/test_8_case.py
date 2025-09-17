import pytest
from datetime import datetime, timedelta, timezone

from repository import filter_components_to_delete
from maven import filter_maven_components_to_delete


def test_case8_repository_no_match_without_rules():
    """
    Ситуация 8 (repository.py):
    Нет правил no-match → все версии без совпадения сохраняются.
    """
    now = datetime.now(timezone.utc)

    components = [
        {
            "name": "test-raw",
            "version": "weird-build-123",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=120)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=90)).isoformat(),
                }
            ],
        },
        {
            "name": "test-docker",
            "version": "strange-456",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=300)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=200)).isoformat(),
                }
            ],
        },
    ]

    regex_rules = {  # правила есть, но версии не подходят
        "^release-.*": {"retention_days": 15, "reserved": 3}
    }

    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=None,
        no_match_reserved=None,
        no_match_min_days_since_last_download=None,
    )

    assert to_delete == []
    for comp in components:
        assert comp["will_delete"] is False
        assert "нет правил no-match" in comp["delete_reason"]


def test_case8_maven_no_match_without_rules():
    """
    Ситуация 8 (maven.py):
    Нет правил no-match → все версии без совпадения сохраняются.
    """
    now = datetime.now(timezone.utc)

    components = [
        {
            "group": "com.example",
            "name": "lib1",
            "version": "strange-1",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=200)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=150)).isoformat(),
                }
            ],
        },
        {
            "group": "com.example",
            "name": "lib2",
            "version": "odd-2",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=400)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=350)).isoformat(),
                }
            ],
        },
    ]

    # Конфиг для maven: snapshot проверяет только SNAPSHOT,
    # release не содержит универсального правила, чтобы получить no-match
    maven_rules = {
        "snapshot": {
            "regex_rules": {
                ".*-SNAPSHOT": {"retention_days": 7, "reserved": 2}
            },
            "no_match_retention_days": None,
            "no_match_reserved": None,
            "no_match_min_days_since_last_download": None,
        },
        "release": {
            "regex_rules": {},  # <-- убрали ".*"
            "no_match_retention_days": None,
            "no_match_reserved": None,
            "no_match_min_days_since_last_download": None,
        },
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)

    assert to_delete == []
    for comp in components:
        assert comp["will_delete"] is False
        assert "нет правил no-match" in comp["delete_reason"]

