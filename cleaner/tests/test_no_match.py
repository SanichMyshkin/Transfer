from repository import filter_components_to_delete
from .test_conf import make_component


def test_retention_applies_no_match():
    components = [make_component("any", "v1", days_old=200, last_download_days=100)]
    to_delete = filter_components_to_delete(
        components,
        {},
        no_match_retention=180,
        no_match_reserved=0,
        no_match_min_days_since_last_download=1,
    )
    assert len(to_delete) == 1


def test_no_match_retention():
    components = [make_component("lib", "nomatch", 15, 10)]
    to_delete = filter_components_to_delete(
        components,
        {},
        no_match_retention=5,
        no_match_reserved=0,
        no_match_min_days_since_last_download=0,
    )
    assert len(to_delete) == 1


def test_no_match_reserved_protection():
    components = [
        make_component("lib", "a", 10),
        make_component("lib", "b", 15),
    ]
    to_delete = filter_components_to_delete(
        components,
        {},
        no_match_retention=5,
        no_match_reserved=1,
        no_match_min_days_since_last_download=0,
    )
    assert len(to_delete) == 1
