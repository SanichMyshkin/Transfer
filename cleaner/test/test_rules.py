import yaml
from common import load_config, get_matching_rule
from repository import filter_components_to_delete
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


def test_load_config_success(tmp_path):
    config_data = {"foo": "bar"}
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(config_data), encoding="utf-8")

    result = load_config(str(config_file))
    assert result == config_data


def test_load_config_file_not_found(tmp_path, caplog):
    result = load_config(str(tmp_path / "missing.yaml"))
    assert result is None
    assert "Ошибка загрузки конфига" in caplog.text


def test_load_config_invalid_yaml(tmp_path, caplog):
    bad_yaml = "{unclosed: [1,2,3}"  # битый YAML
    config_file = tmp_path / "bad.yaml"
    config_file.write_text(bad_yaml, encoding="utf-8")

    result = load_config(str(config_file))
    assert result is None
    assert "Ошибка загрузки конфига" in caplog.text


def test_get_matching_rule_with_match():
    rules = {
        ".*-snapshot": {"retention_days": 30, "reserved": 1},
        ".*-snapshot-extra": {"retention_days": 10},  # более длинный паттерн
    }
    pattern, retention, reserved, min_days = get_matching_rule(
        "1.0-SNAPSHOT-EXTRA",   # внутри функции станет "1.0-snapshot-extra"
        rules,
        no_match_retention=90,
        no_match_reserved=2,
        no_match_min_days_since_last_download=15,
    )
    assert pattern == ".*-snapshot-extra"   # теперь совпадёт
    assert retention.days == 10
    assert reserved is None
    assert min_days is None


def test_get_matching_rule_no_match():
    rules = {"^2\\..*": {"retention_days": 5}}
    pattern, retention, reserved, min_days = get_matching_rule(
        "1.0.0",
        rules,
        no_match_retention=42,
        no_match_reserved=7,
        no_match_min_days_since_last_download=3,
    )
    assert pattern == "no-match"
    assert retention.days == 42
    assert reserved == 7
    assert min_days == 3
