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


# ======== TEST CASE 13: MATCH ========
def test_repository_case13_match():
    components = [
        make_component("compA", "dev-1", last_modified_days_ago=1),   # reserved
        make_component("compA", "dev-2", last_modified_days_ago=2),   # reserved
        make_component("compA", "dev-3", last_modified_days_ago=5),   # age=5 < 7 → сохраняем
        make_component("compA", "dev-4", last_modified_days_ago=12),  # age=12 > 7 → удаляем
    ]

    regex_rules = {
        "^dev-": {"retention_days": 7, "reserved": 2}
    }

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=None,   # ❌
        no_match_reserved=1,       # ✅
        no_match_min_days_since_last_download=3,  # ✅
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    assert set(saved_versions) == {"dev-1", "dev-2", "dev-3"}
    assert set(deleted_versions) == {"dev-4"}


# ======== TEST CASE 13: NO-MATCH ========
def test_repository_case13_no_match():
    components = [
        make_component("compB", "random-1", last_modified_days_ago=1, last_download_days_ago=1),   # reserved
        make_component("compB", "random-2", last_modified_days_ago=2, last_download_days_ago=2),   # dl=2 < 3 → сохраняем
        make_component("compB", "random-3", last_modified_days_ago=10, last_download_days_ago=5),  # dl=5 > 3 → удаляем
    ]

    regex_rules = {
        "^dev-": {"retention_days": 7, "reserved": 2}  # не подходит для random-*
    }

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=None,   # ❌
        no_match_reserved=1,       # ✅
        no_match_min_days_since_last_download=3,  # ✅
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # reserved → random-1
    # random-2 → last_download=2 < min_days=3 → сохраняем
    # random-3 → last_download=5 > 3 → удаляем
    assert set(saved_versions) == {"random-1", "random-2"}
    assert set(deleted_versions) == {"random-3"}
