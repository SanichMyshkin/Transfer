from datetime import datetime, timedelta, timezone
from maven import filter_maven_components_to_delete


def make_component(group, artifact, version, last_modified, last_download=None):
    c = {
        "id": f"{group}:{artifact}:{version}",
        "group": group,
        "name": artifact,
        "version": version,
        "assets": [
            {"lastModified": last_modified}
        ],
    }
    if last_download:
        c["assets"][0]["lastDownloaded"] = last_download
    return c


def test_maven_reserved_releases():
    now = datetime.now(timezone.utc)
    comps = [
        make_component("com.example", "lib", "1.0", (now - timedelta(days=5)).isoformat()),
        make_component("com.example", "lib", "1.1", (now - timedelta(days=1)).isoformat()),
    ]
    rules = {
        "release": {"regex_rules": {".*": {"reserved": 1}}}
    }

    deleted = filter_maven_components_to_delete(comps, rules)
    # Один из двух должен удалиться, второй зарезервирован
    assert len(deleted) == 1
    assert deleted[0]["version"] == "1.0"


def test_maven_snapshot_retention():
    now = datetime.now(timezone.utc)
    old = (now - timedelta(days=90)).isoformat()
    comps = [
        make_component("com.example", "lib", "2.0-SNAPSHOT", old)
    ]
    rules = {
        "snapshot": {"regex_rules": {".*": {"retention_days": 30}}}
    }

    deleted = filter_maven_components_to_delete(comps, rules)
    assert len(deleted) == 1
    assert deleted[0]["version"] == "2.0-SNAPSHOT"


def test_maven_snapshot_recent_download_saved():
    now = datetime.now(timezone.utc)
    comps = [
        make_component(
            "com.example", "lib", "2.1-SNAPSHOT",
            (now - timedelta(days=40)).isoformat(),
            (now - timedelta(days=2)).isoformat(),
        )
    ]
    rules = {
        "snapshot": {"regex_rules": {".*": {"retention_days": 30, "min_days_since_last_download": 10}}}
    }

    deleted = filter_maven_components_to_delete(comps, rules)
    # Должен сохраниться, т.к. недавно скачивали
    assert len(deleted) == 0


def test_maven_release_deleted_if_old_and_not_downloaded():
    now = datetime.now(timezone.utc)
    comps = [
        make_component(
            "com.example", "lib", "3.0",
            (now - timedelta(days=400)).isoformat(),
            (now - timedelta(days=400)).isoformat(),
        )
    ]
    rules = {
        "release": {"regex_rules": {".*": {"retention_days": 365}}}
    }

    deleted = filter_maven_components_to_delete(comps, rules)
    assert len(deleted) == 1
    assert deleted[0]["version"] == "3.0"


def test_maven_keep_if_within_retention():
    now = datetime.now(timezone.utc)
    comps = [
        make_component("com.example", "lib", "3.1", (now - timedelta(days=10)).isoformat())
    ]
    rules = {
        "release": {"regex_rules": {".*": {"retention_days": 30}}}
    }

    deleted = filter_maven_components_to_delete(comps, rules)
    assert len(deleted) == 0


def test_maven_multiple_versions_grouping_and_reserved():
    now = datetime.now(timezone.utc)
    comps = [
        make_component("com.example", "lib", "1.0", (now - timedelta(days=200)).isoformat()),
        make_component("com.example", "lib", "1.1", (now - timedelta(days=100)).isoformat()),
        make_component("com.example", "lib", "1.2", (now - timedelta(days=10)).isoformat()),
    ]
    rules = {
        "release": {"regex_rules": {".*": {"reserved": 2, "retention_days": 30}}}
    }

    deleted = filter_maven_components_to_delete(comps, rules)
    # 1.2 и 1.1 зарезервированы, 1.0 устарел и удаляется
    assert len(deleted) == 1
    assert deleted[0]["version"] == "1.0"


def test_maven_component_without_last_modified_skipped():
    comp = {
        "id": "broken:lib:0.1",
        "group": "broken",
        "name": "lib",
        "version": "0.1",
        "assets": [{"path": "some/path.jar"}],  # нет lastModified
    }
    rules = {"release": {"regex_rules": {".*": {"retention_days": 30}}}}

    deleted = filter_maven_components_to_delete([comp], rules)
    # Такой компонент должен быть проигнорирован
    assert len(deleted) == 0


def test_maven_timestamped_snapshot_detected_and_deleted():
    now = datetime.now(timezone.utc)
    comps = [
        make_component(
            "com.example", "lib", "1.0-20250829.123456-1",
            (now - timedelta(days=60)).isoformat()
        )
    ]
    rules = {
        "snapshot": {"regex_rules": {".*": {"retention_days": 30}}}
    }
    deleted = filter_maven_components_to_delete(comps, rules)
    assert len(deleted) == 1
    assert deleted[0]["version"].startswith("1.0-2025")


def test_maven_no_matching_rule_uses_fallback():
    now = datetime.now(timezone.utc)
    comps = [
        make_component("com.example", "lib", "9.9", (now - timedelta(days=100)).isoformat())
    ]
    rules = {
        "release": {
            "regex_rules": {},  # нет правил
            "no_match_retention_days": 30,
        }
    }
    deleted = filter_maven_components_to_delete(comps, rules)
    assert len(deleted) == 1


def test_maven_reserved_overrides_retention():
    now = datetime.now(timezone.utc)
    comps = [
        make_component("com.example", "lib", "1.0", (now - timedelta(days=999)).isoformat()),
        make_component("com.example", "lib", "1.1", (now - timedelta(days=998)).isoformat()),
    ]
    rules = {
        "release": {"regex_rules": {".*": {"reserved": 1, "retention_days": 30}}}
    }
    deleted = filter_maven_components_to_delete(comps, rules)
    # Только старейший удаляется, потому что один зарезервирован
    assert len(deleted) == 1
    assert deleted[0]["version"] == "1.0"


def test_maven_keep_if_recently_downloaded_even_if_old():
    now = datetime.now(timezone.utc)
    comps = [
        make_component(
            "com.example", "lib", "5.0",
            (now - timedelta(days=400)).isoformat(),
            (now - timedelta(days=1)).isoformat()
        )
    ]
    rules = {
        "release": {"regex_rules": {".*": {"retention_days": 30, "min_days_since_last_download": 10}}}
    }
    deleted = filter_maven_components_to_delete(comps, rules)
    assert len(deleted) == 0


def test_maven_multiple_groups_independent():
    now = datetime.now(timezone.utc)
    comps = [
        make_component("com.a", "lib", "1.0", (now - timedelta(days=200)).isoformat()),
        make_component("com.b", "lib", "1.0", (now - timedelta(days=200)).isoformat()),
    ]
    rules = {
        "release": {"regex_rules": {".*": {"reserved": 1, "retention_days": 30}}}
    }
    deleted = filter_maven_components_to_delete(comps, rules)
    # У каждой группы своя "резервация"
    assert len(deleted) == 0


def test_maven_component_with_invalid_last_modified_skipped():
    comp = {
        "id": "broken:lib:0.2",
        "group": "broken",
        "name": "lib",
        "version": "0.2",
        "assets": [{"lastModified": "not-a-date"}],
    }
    rules = {"release": {"regex_rules": {".*": {"retention_days": 30}}}}

    deleted = filter_maven_components_to_delete([comp], rules)
    assert len(deleted) == 0


def test_maven_component_with_invalid_last_download_ignored():
    now = datetime.now(timezone.utc)
    comp = {
        "id": "broken:lib:0.3",
        "group": "broken",
        "name": "lib",
        "version": "0.3",
        "assets": [
            {"lastModified": (now - timedelta(days=100)).isoformat(),
             "lastDownloaded": "not-a-date"}
        ],
    }
    rules = {"release": {"regex_rules": {".*": {"retention_days": 30}}}}
    deleted = filter_maven_components_to_delete([comp], rules)
    # должно удалиться, потому что lastDownloaded некорректный, и retention истёк
    assert len(deleted) == 1
    assert deleted[0]["version"] == "0.3"


def test_maven_component_without_version_skipped():
    comp = {
        "id": "broken:lib",
        "group": "broken",
        "name": "lib",
        "assets": [{"lastModified": datetime.now(timezone.utc).isoformat()}],
    }
    rules = {"release": {"regex_rules": {".*": {"retention_days": 30}}}}
    deleted = filter_maven_components_to_delete([comp], rules)
    assert len(deleted) == 0
