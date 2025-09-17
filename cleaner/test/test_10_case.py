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


# ======== TEST CASE 10: MATCH ========
def test_repository_case10_match():
    components = [
        make_component("compA", "dev-1", last_modified_days_ago=2),   # свежий, попадет в reserved
        make_component("compA", "dev-2", last_modified_days_ago=10),  # старый, удалится
        make_component("compA", "dev-3", last_modified_days_ago=5),   # свежий, попадет в reserved
        make_component("compA", "dev-4", last_modified_days_ago=6),   # возраст=6 <= retention=7 → сохранится
    ]

    regex_rules = {
        "^dev-": {"retention_days": 7, "reserved": 2}
    }

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=5,
        no_match_reserved=1,
        no_match_min_days_since_last_download=None,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # sorted по last_modified: dev-1 (2 дн), dev-3 (5 дн), dev-4 (6 дн), dev-2 (10 дн)
    # reserved=2 → dev-1, dev-3
    # dev-4 → age=6 <= retention=7 → сохраняем
    # dev-2 → age=10 > retention=7 → удаляем
    assert set(saved_versions) == {"dev-1", "dev-3", "dev-4"}
    assert set(deleted_versions) == {"dev-2"}


# ======== TEST CASE 10: NO-MATCH ========
def test_repository_case10_no_match():
    components = [
        make_component("compB", "random-1", last_modified_days_ago=1),   # попадет в reserved
        make_component("compB", "random-2", last_modified_days_ago=3),   # age=3 <= retention=5 → сохранится
        make_component("compB", "random-3", last_modified_days_ago=10),  # age=10 > retention=5 → удалится
    ]

    regex_rules = {
        "^dev-": {"retention_days": 7, "reserved": 2}  # не сработает для random-*
    }

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=5,
        no_match_reserved=1,
        no_match_min_days_since_last_download=None,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # sorted по last_modified: random-1 (1 дн), random-2 (3 дн), random-3 (10 дн)
    # reserved=1 → random-1
    # random-2 → age=3 <= retention=5 → сохраняем
    # random-3 → age=10 > retention=5 → удаляем
    assert set(saved_versions) == {"random-1", "random-2"}
    assert set(deleted_versions) == {"random-3"}
