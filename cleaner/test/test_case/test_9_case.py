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
def test_repository_ninth_case():
    components = [
        make_component("compX", "dev-1", last_modified_days_ago=2),
        make_component("compX", "dev-2", last_modified_days_ago=6),
        make_component("compX", "dev-3", last_modified_days_ago=10),
    ]

    regex_rules = {"^dev-": {"retention_days": 5, "reserved": 1}}

    # no-match правила присутствуют, но тест проверяет только match
    no_match_retention = 10
    no_match_reserved = 1
    no_match_min_days_since_last_download = 7

    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention,
        no_match_reserved,
        no_match_min_days_since_last_download,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # Сортировка по last_modified: dev-1 (2 дня), dev-2 (6 дней), dev-3 (10 дней)
    # reserved оставит dev-1
    # dev-2 → age=6 > retention(5) → удаляется
    # dev-3 → age=10 > retention(5) → удаляется
    assert set(saved_versions) == {"dev-1"}
    assert set(deleted_versions) == {"dev-2", "dev-3"}


# ======== TEST: maven.py ========
def test_maven_ninth_case():
    components = [
        {
            "group": "org.test",
            "name": "libX",
            "version": "1.0.0",
            "assets": [
                {
                    "lastModified": (
                        datetime.now(timezone.utc) - timedelta(days=2)
                    ).isoformat()
                }
            ],
        },
        {
            "group": "org.test",
            "name": "libX",
            "version": "1.0.1",
            "assets": [
                {
                    "lastModified": (
                        datetime.now(timezone.utc) - timedelta(days=6)
                    ).isoformat()
                }
            ],
        },
        {
            "group": "org.test",
            "name": "libX",
            "version": "1.0.2",
            "assets": [
                {
                    "lastModified": (
                        datetime.now(timezone.utc) - timedelta(days=10)
                    ).isoformat()
                }
            ],
        },
    ]

    maven_rules = {
        "release": {
            "regex_rules": {".*": {"retention_days": 5, "reserved": 1}},
            "no_match_retention_days": 10,
            "no_match_reserved": 1,
            "no_match_min_days_since_last_download": 7,
        }
    }

    to_delete = filter_maven_components_to_delete(components, maven_rules)

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # Сортировка по last_modified: 1.0.0 (2 дня), 1.0.1 (6 дней), 1.0.2 (10 дней)
    # reserved оставит 1.0.0
    # 1.0.1 → age=6 > retention(5) → удаляется
    # 1.0.2 → age=10 > retention(5) → удаляется
    assert set(saved_versions) == {"1.0.0"}
    assert set(deleted_versions) == {"1.0.1", "1.0.2"}


import pytest
from datetime import datetime, timedelta, timezone

from repository import filter_components_to_delete


def make_component(name, version, last_modified_days_ago, last_download_days_ago=None):
    now = datetime.now(timezone.utc)
    last_modified = now - timedelta(days=last_modified_days_ago)
    asset = {"lastModified": last_modified.isoformat()}
    if last_download_days_ago is not None:
        last_download = now - timedelta(days=last_download_days_ago)
        asset["lastDownloaded"] = last_download.isoformat()
    return {"name": name, "version": version, "assets": [asset]}


# ======== TEST CASE 9: MATCH ========
def test_repository_case9_match():
    components = [
        make_component(
            "compA", "dev-1", last_modified_days_ago=2
        ),  # свежий, попадет в reserved
        make_component(
            "compA", "dev-2", last_modified_days_ago=10
        ),  # старый, должен удалиться
        make_component(
            "compA", "dev-3", last_modified_days_ago=4
        ),  # свежий, попадет в reserved
    ]

    regex_rules = {"^dev-": {"retention_days": 7, "reserved": 2}}

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=5,  # для этого теста не влияет
        no_match_reserved=1,
        no_match_min_days_since_last_download=3,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # sorted по last_modified: dev-1 (2 дня), dev-3 (4 дня), dev-2 (10 дней)
    # top-2 reserved → dev-1 и dev-3
    assert set(saved_versions) == {"dev-1", "dev-3"}
    assert set(deleted_versions) == {"dev-2"}


# ======== TEST CASE 9: NO-MATCH ========
def test_repository_case9_no_match():
    components = [
        make_component(
            "compB", "random-1", last_modified_days_ago=1
        ),  # сохранится по reserved
        make_component(
            "compB", "random-2", last_modified_days_ago=3
        ),  # свежий → retention=5
        make_component(
            "compB", "random-3", last_modified_days_ago=10, last_download_days_ago=2
        ),  # сохранится по min_days=3
        make_component(
            "compB", "random-4", last_modified_days_ago=15, last_download_days_ago=20
        ),  # старый → удалится
    ]

    regex_rules = {
        "^dev-": {
            "retention_days": 7,
            "reserved": 2,
        }  # не сработает, т.к. random-* не матчится
    }

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=5,  # ✅
        no_match_reserved=1,  # ✅
        no_match_min_days_since_last_download=3,  # ✅
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # sorted по last_modified: random-1 (1 дн), random-2 (3 дн), random-3 (10 дн), random-4 (15 дн)
    # top-1 reserved → random-1
    # random-2 → age=3 <= retention=5 → сохраняем
    # random-3 → age=10 > retention, но last_download=2 дн → сохраняем
    # random-4 → age=15 > retention=5 и last_download=20 > min_days=3 → удаляем
    assert set(saved_versions) == {"random-1", "random-2", "random-3"}
    assert set(deleted_versions) == {"random-4"}
