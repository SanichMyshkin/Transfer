from repository import filter_components_to_delete
from .test_conf import make_component


# 9. Не удаляется latest
def test_latest_not_deleted():
    components = [make_component("lib", "latest", 100)]
    to_delete = filter_components_to_delete(
        components,
        {},
        no_match_retention=1,
        no_match_reserved=0,
        no_match_min_days_since_last_download=0,
    )
    assert len(to_delete) == 0


def test_ignore_no_assets():
    c = {"id": "1", "name": "lib", "version": "v1", "assets": []}
    to_delete = filter_components_to_delete(
        [c],
        {},
        no_match_retention=10,
        no_match_reserved=0,
        no_match_min_days_since_last_download=0,
    )
    assert len(to_delete) == 0


def test_ignore_no_last_modified():
    c = {"id": "1", "name": "lib", "version": "v1", "assets": [{}]}
    to_delete = filter_components_to_delete(
        [c],
        {},
        no_match_retention=10,
        no_match_reserved=0,
        no_match_min_days_since_last_download=0,
    )
    assert len(to_delete) == 0


def test_ignore_missing_name_version():
    c = make_component(None, None, 100)
    c.pop("name")
    c.pop("version")
    to_delete = filter_components_to_delete(
        [c],
        {},
        no_match_retention=10,
        no_match_reserved=0,
        no_match_min_days_since_last_download=0,
    )
    assert len(to_delete) == 0


def test_grouping_by_name_and_pattern():
    components = [
        make_component("pkg", "dev-1", 20),
        make_component("pkg", "dev-2", 30),
        make_component("pkg", "rel-1", 40),
    ]
    regex_rules = {
        "^dev-.*": {"reserved": 1},
        "^rel-.*": {"reserved": 1},
    }
    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=12,
        no_match_reserved=0,
        no_match_min_days_since_last_download=0,
    )
    assert len(to_delete) == 1
    assert to_delete[0]["version"] == "dev-2"


def test_missing_last_download_with_min_days_rule():
    components = [make_component("lib", "v1", 15, None)]
    regex_rules = {".*": {"retention_days": 10, "min_days_since_last_download": 1}}
    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=12,
        no_match_reserved=0,
        no_match_min_days_since_last_download=0,
    )
    assert len(to_delete) == 1
