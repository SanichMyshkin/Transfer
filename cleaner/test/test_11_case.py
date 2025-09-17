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


# ======== TEST CASE 11: MATCH ========
def test_repository_case11_match():
    components = [
        make_component(
            "compA", "dev-1", last_modified_days_ago=1
        ),  # попадет в reserved
        make_component(
            "compA", "dev-2", last_modified_days_ago=2
        ),  # попадет в reserved
        make_component(
            "compA", "dev-3", last_modified_days_ago=6
        ),  # age=6 <= retention=7 → сохранится
        make_component(
            "compA", "dev-4", last_modified_days_ago=12
        ),  # age=12 > retention=7 → удалится
    ]

    regex_rules = {"^dev-": {"retention_days": 7, "reserved": 2}}

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=5,
        no_match_reserved=None,
        no_match_min_days_since_last_download=3,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # sorted по last_modified: dev-1 (1 дн), dev-2 (2 дн), dev-3 (6 дн), dev-4 (12 дн)
    # reserved=2 → dev-1, dev-2
    # dev-3 → age=6 <= retention=7 → сохраняем
    # dev-4 → age=12 > retention=7 → удаляем
    assert set(saved_versions) == {"dev-1", "dev-2", "dev-3"}
    assert set(deleted_versions) == {"dev-4"}


# ======== TEST CASE 11: NO-MATCH ========
def test_repository_case11_no_match():
    components = [
        make_component(
            "compB", "random-1", last_modified_days_ago=2, last_download_days_ago=1
        ),  # age=2 ≤ 5 → сохранится
        make_component(
            "compB", "random-2", last_modified_days_ago=8, last_download_days_ago=2
        ),  # age=8 > 5, но dl=2 ≤ 3 → сохранится
        make_component(
            "compB", "random-3", last_modified_days_ago=10, last_download_days_ago=15
        ),  # age=10 > 5, dl=15 > 3 → удалится
    ]

    regex_rules = {
        "^dev-": {"retention_days": 7, "reserved": 2}  # не подходит
    }

    to_delete = filter_components_to_delete(
        components,
        regex_rules=regex_rules,
        no_match_retention=5,
        no_match_reserved=None,
        no_match_min_days_since_last_download=3,
    )

    saved_versions = [c["version"] for c in components if not c["will_delete"]]
    deleted_versions = [c["version"] for c in to_delete]

    # random-1 → age=2 <= 5 → сохраняем
    # random-2 → age=8 > 5, но dl=2 <= 3 → сохраняем
    # random-3 → age=10 > 5, dl=15 > 3 → удаляем
    assert set(saved_versions) == {"random-1", "random-2"}
    assert set(deleted_versions) == {"random-3"}
