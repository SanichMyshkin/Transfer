# tests/test_case53.py
from datetime import datetime, timedelta, timezone

from repository import filter_components_to_delete
from maven import filter_maven_components_to_delete


def make_component(name, version, last_modified_days_ago, last_download_days_ago=None):
    """
    Утилита для сборки тестовых компонентов (repository).
    """
    now = datetime.now(timezone.utc)
    last_modified = now - timedelta(days=last_modified_days_ago)
    asset = {"lastModified": last_modified.isoformat()}
    if last_download_days_ago is not None:
        last_download = now - timedelta(days=last_download_days_ago)
        asset["lastDownloaded"] = last_download.isoformat()
    return {"name": name, "version": version, "assets": [asset]}


# ===================== CASE 53 =====================
def test_repository_case53():
    components = [
        # MATCH: без reserved, только min_days
        make_component("compA", "dev-1", 10, 2),  # dl=2 <= 5 → сохраняем
        make_component("compA", "dev-2", 10, 20),  # dl=20 > 5 → удаляется
        # NO-MATCH: reserved=1, min_days=5
        make_component(
            "compB", "random-1", 1, 30
        ),  # самый свежий → reserved → сохраняем
        make_component("compB", "random-2", 5, 2),  # dl=2 <= 5 → сохраняем
        make_component("compB", "random-3", 20, 20),  # dl=20 > 5 → удаляется
    ]

    regex_rules = {"^dev-": {"min_days_since_last_download": 5}}

    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=None,  # ❌ retention отсутствует
        no_match_reserved=1,  # ✅ reserved включён
        no_match_min_days_since_last_download=5,  # ✅ min_days включён
    )

    saved = [c["version"] for c in components if not c["will_delete"]]
    deleted = [c["version"] for c in to_delete]

    assert set(saved) == {"dev-1", "random-1", "random-2"}
    assert set(deleted) == {"dev-2", "random-3"}


def test_maven_case53():
    now = datetime.now(timezone.utc)
    components = [
        # MATCH
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.0",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=10)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=2)).isoformat(),
                }
            ],
        },  # dl=2 <= 5 → сохраняем
        {
            "group": "org.test",
            "name": "libA",
            "version": "1.0.1",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=10)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=20)).isoformat(),
                }
            ],
        },  # dl=20 > 5 → удаляется
        # NO-MATCH
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.0",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=10)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=30)).isoformat(),
                }
            ],
        },  # reserved → сохраняем
        {
            "group": "org.test",
            "name": "libB",
            "version": "2.0.1",
            "assets": [
                {
                    "lastModified": (now - timedelta(days=10)).isoformat(),
                    "lastDownloaded": (now - timedelta(days=2)).isoformat(),
                }
            ],
        },  # dl=2 <= 5 → сохраняем
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
        },  # dl=20 > 5 → удаляется
    ]

    maven_rules = {
        "release": {
            "regex_rules": {"^1.*": {"min_days_since_last_download": 5}},
            "no_match_retention_days": None,  # ❌ retention отсутствует
            "no_match_reserved": 1,  # ✅ reserved включён
            "no_match_min_days_since_last_download": 5,  # ✅ min_days включён
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)

    saved = [c["version"] for c in components if not c["will_delete"]]
    deleted = [c["version"] for c in to_delete]

    assert set(saved) == {"1.0.0", "2.0.0", "2.0.1"}
    assert set(deleted) == {"1.0.1", "2.0.2"}
