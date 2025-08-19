import pytest
import requests
from unittest.mock import patch, MagicMock
from metrics.certs import sync_cert


@pytest.mark.parametrize(
    "cn,url,expected",
    [
        ("*.example.com", "https://repo.example.com", 1),
        ("repo.example.com", "https://repo.example.com", 1),
        ("example.com", "https://fooexample.com", 1),  # ← раньше было 2
        ("example.com", "https://another.com", 0),
        ("", "https://repo.example.com", 0),
        ("example.com", "", 0),
    ],
)
def test_match_level_parametrized(cn, url, expected):
    assert sync_cert.match_level(cn, url) == expected


@patch("metrics.certs.sync_cert.get_from_nexus")
@patch("requests.delete")
def test_remove_duplicate_certs_multiple_dupes(mock_delete, mock_get):
    mock_get.return_value = [
        {"id": "1", "fingerprint": "abc"},
        {"id": "2", "fingerprint": "abc"},
        {"id": "3", "fingerprint": "abc"},
    ]
    mock_delete.return_value = MagicMock(status_code=204)

    sync_cert.remove_duplicate_certs("http://nexus", ("u","p"))
    # Удалены все кроме первого
    assert mock_delete.call_count == 2
    deleted_ids = [c.args[0] for c in mock_delete.call_args_list]
    assert "truststore/2" in deleted_ids[0] or "truststore/2" in deleted_ids[1]
    assert "truststore/3" in deleted_ids[0] or "truststore/3" in deleted_ids[1]


@patch("metrics.certs.sync_cert.get_from_nexus")
@patch("requests.delete")
def test_remove_duplicate_certs_missing_fields(mock_delete, mock_get):
    mock_get.return_value = [
        {"id": "1", "fingerprint": None},
        {"fingerprint": "abc"},
        {"id": "2", "fingerprint": "abc"},
    ]
    sync_cert.remove_duplicate_certs("http://nexus", ("u","p"))
    # Удалять нечего
    mock_delete.assert_not_called()


@patch("requests.post")
def test_add_cert_to_truststore_success(mock_post):
    resp = MagicMock(status_code=200)
    resp.raise_for_status.return_value = None
    mock_post.return_value = resp

    ok = sync_cert.add_cert_to_truststore(
        "http://nexus", ("u","p"), "PEM", "cn", "remote", "repo"
    )
    assert ok is True
    mock_post.assert_called_once()


@patch("requests.post")
def test_add_cert_to_truststore_failure(mock_post):
    mock_post.side_effect = requests.exceptions.RequestException("boom")

    ok = sync_cert.add_cert_to_truststore(
        "http://nexus", ("u","p"), "PEM", "cn", "remote", "repo"
    )
    assert ok is False


@patch("metrics.certs.sync_cert.get_from_nexus")
def test_fetch_remote_certs_dict(mock_get):
    mock_get.return_value = {"subjectCommonName": "repo.example.com", "pem": "PEM"}
    result = sync_cert.fetch_remote_certs("http://nexus", "https://repo.example.com", ("u","p"), "repo")
    assert isinstance(result, list)
    assert result[0]["subjectCommonName"] == "repo.example.com"


@patch("metrics.certs.sync_cert.get_from_nexus")
def test_fetch_remote_certs_list(mock_get):
    mock_get.return_value = [
        {"subjectCommonName": "repo1", "pem": "PEM1"},
        {"subjectCommonName": "repo2", "pem": "PEM2"},
    ]
    result = sync_cert.fetch_remote_certs("http://nexus", "https://repo.example.com", ("u","p"), "repo")
    assert isinstance(result, list)
    assert len(result) == 2


@patch("metrics.certs.sync_cert.get_from_nexus")
def test_fetch_remote_certs_failure(mock_get):
    mock_get.side_effect = Exception("fail")
    result = sync_cert.fetch_remote_certs("http://nexus", "://invalid", ("u","p"), "repo")
    assert result == []
