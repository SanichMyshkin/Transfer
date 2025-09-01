from unittest.mock import patch
from datetime import datetime, timezone
from repository import filter_components_to_delete   # заменили cleaner на repository
from .test_conf import make_component


@patch("repository.datetime")   # указываем новый путь
def test_strict_min_days_since_last_download(mock_datetime):
    fixed_now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    mock_datetime.now.return_value = fixed_now
    mock_datetime.side_effect = lambda *args, **kwargs: datetime(
        *args, **kwargs
    )  # чтобы parse работал корректно
    mock_datetime.timezone = timezone

    components = [
        make_component("lib", "v1", 10, last_download_days=3),
        make_component("lib", "v2", 10, last_download_days=4),
    ]
    regex_rules = {".*": {"min_days_since_last_download": 3}}
    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=300,
        no_match_reserved=0,
        no_match_min_days_since_last_download=3,
    )
    assert len(to_delete) == 1
    assert to_delete[0]["version"] == "v2"


def test_specific_regex_priority():
    components = [
        make_component("lib", "dev-latest.20250601", 15, 10),  # попадает под оба
    ]
    regex_rules = {
        "^dev-.*": {"retention_days": 10},
        "dev-latest.*": {"min_days_since_last_download": 5},
    }

    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=20,
        no_match_reserved=0,
        no_match_min_days_since_last_download=0,
    )

    # Скачан 10 дней назад, а min_days_since_last_download = 5 → удаляется
    assert len(to_delete) == 1


def test_hello_world_does_not_match_suffix():
    components = [make_component("lib", "some-hello-world", 15, 10)]
    regex_rules = {
        "^hello-.*": {"retention_days": 10},
        "hello-world.*": {"min_days_since_last_download": 5},
    }

    to_delete = filter_components_to_delete(
        components,
        regex_rules,
        no_match_retention=5,
        no_match_reserved=0,
        no_match_min_days_since_last_download=0,
    )

    # Ни один паттерн не подходит → применяется no_match_retention = 5 → удаляется
    assert len(to_delete) == 1
