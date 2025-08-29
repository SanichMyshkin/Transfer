from cleaner import filter_components_to_delete
from .test_conf import make_component


def test_all_rules_applied():
    components = [
        make_component("lib", "dev-1", days_old=10, last_download_days=5),  # удалится
        make_component(
            "lib", "dev-2", days_old=1, last_download_days=0
        ),  # останется (скачан недавно)
    ]
    regex_rules = {
        "^dev-.*": {
            "retention_days": 7,
            "reserved": 1,
            "min_days_since_last_download": 3,
        }
    }
    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=12,
        no_match_reserved=1,
        no_match_min_days_since_last_download=2,
    )
    assert len(to_delete) == 1
    assert to_delete[0]["version"] == "dev-1"


def test_only_reserved():
    components = [
        make_component("lib", "r1", 5),  # свежий
        make_component("lib", "r2", 10),  # старше — должен быть удалён
    ]
    regex_rules = {"^r.*": {"reserved": 1}}
    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=None,
        no_match_reserved=None,
        no_match_min_days_since_last_download=None,
    )
    # reserved=1 → сохраняется только r1 (самый свежий)
    # r2 должен быть удалён
    assert len(to_delete) == 1
    assert to_delete[0]["version"] == "r2"


def test_only_retention():
    components = [make_component("lib", "old", 20)]
    regex_rules = {".*": {"retention_days": 10}}
    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=300,
        no_match_reserved=0,
        no_match_min_days_since_last_download=0,
    )
    assert len(to_delete) == 1


def test_only_min_days_since_last_download():
    components = [make_component("lib", "v", 5, last_download_days=1)]
    regex_rules = {".*": {"min_days_since_last_download": 3}}
    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=300,
        no_match_reserved=0,
        no_match_min_days_since_last_download=0,
    )
    assert len(to_delete) == 1


def test_retention_and_reserved():
    components = [
        make_component("lib", "v1", 15),
        make_component("lib", "v2", 20),
    ]
    regex_rules = {".*": {"retention_days": 10, "reserved": 1}}
    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=12,
        no_match_reserved=1,
        no_match_min_days_since_last_download=1,
    )
    assert len(to_delete) == 1
    assert to_delete[0]["version"] == "v2"


def test_retention_and_min_download():
    components = [make_component("lib", "v1", 15, 10)]
    regex_rules = {".*": {"retention_days": 5, "min_days_since_last_download": 7}}
    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=20,
        no_match_reserved=0,
        no_match_min_days_since_last_download=1,
    )
    assert len(to_delete) == 1


def test_reserved_and_min_download():
    comps = [
        make_component("lib", "v1", 5, 1),  # в reserved — сохраняется
        make_component(
            "lib", "v2", 10, 10
        ),  # не в reserved, скачан 10 дней назад → удаляется
    ]
    rules = {".*": {"reserved": 1, "min_days_since_last_download": 3}}

    deleted = filter_components_to_delete(comps, rules, 10, 0, 0)

    assert [d["version"] for d in deleted] == ["v2"]
