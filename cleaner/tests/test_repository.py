import logging
import requests
import logging
from repository import (
    get_repository_format,
    get_repository_items,
    convert_raw_assets_to_components,
    delete_component,
    filter_components_to_delete,
    clear_repository,
)


def test_delete_component_success(monkeypatch, caplog):
    caplog.set_level(logging.INFO)

    def fake_delete(url, auth, timeout, verify):
        class R:
            status_code = 200

            def raise_for_status(self):
                pass

        return R()

    monkeypatch.setattr(requests, "delete", fake_delete)

    delete_component("123", "pkg", "v1", dry_run=False)
    assert "✅ Удалён" in caplog.text


def test_delete_component_dry_run(monkeypatch, caplog):
    caplog.set_level(logging.INFO)

    called = {"delete": False}

    def fake_delete(*a, **k):
        called["delete"] = True

    monkeypatch.setattr(requests, "delete", fake_delete)

    delete_component("123", "pkg", "v1", dry_run=True)
    assert "DRY_RUN" in caplog.text
    assert called["delete"] is False


# ===== get_repository_format =====
def test_get_repository_format_found(monkeypatch):
    def fake_get(*a, **k):
        class R:
            def raise_for_status(self):
                pass

            def json(self):
                return [{"name": "repo1", "format": "raw"}]

        return R()

    monkeypatch.setattr(requests, "get", fake_get)
    assert get_repository_format("repo1") == "raw"


def test_get_repository_format_not_found(monkeypatch):
    def fake_get(*a, **k):
        class R:
            def raise_for_status(self):
                pass

            def json(self):
                return [{"name": "other", "format": "docker"}]

        return R()

    monkeypatch.setattr(requests, "get", fake_get)
    assert get_repository_format("missing") is None


def test_get_repository_format_error(monkeypatch, caplog):
    caplog.set_level(logging.ERROR)

    def fake_get(*a, **k):
        raise requests.RequestException("boom")

    monkeypatch.setattr(requests, "get", fake_get)
    assert get_repository_format("repo1") is None
    assert "Не удалось определить формат" in caplog.text


# ===== get_repository_items =====
def test_get_repository_items_paged(monkeypatch):
    calls = {"n": 0}

    def fake_get(*a, **k):
        calls["n"] += 1

        class R:
            def raise_for_status(self):
                pass

            def json(self):
                if calls["n"] == 1:
                    return {"items": [{"id": 1}], "continuationToken": "next"}
                return {"items": [{"id": 2}], "continuationToken": None}

        return R()

    monkeypatch.setattr(requests, "get", fake_get)
    items = get_repository_items("repo", "raw")
    assert [i["id"] for i in items] == [1, 2]


def test_get_repository_items_error(monkeypatch, caplog):
    caplog.set_level(logging.ERROR)

    def fake_get(*a, **k):
        raise requests.RequestException("fail")

    monkeypatch.setattr(requests, "get", fake_get)
    items = get_repository_items("repo", "raw")
    assert items == []
    assert "Ошибка при получении данных" in caplog.text


# ===== convert_raw_assets_to_components =====
def test_convert_raw_assets_skips_empty_version():
    assets = [{"id": "1", "path": "folder/"}]  # basename(path) пустой
    comps = convert_raw_assets_to_components(assets)
    assert comps == []


# ===== delete_component =====
def test_delete_component_request_exception(monkeypatch, caplog):
    caplog.set_level(logging.ERROR)

    def fake_delete(*a, **k):
        raise requests.RequestException("oops")

    monkeypatch.setattr(requests, "delete", fake_delete)
    delete_component("id1", "pkg", "v1", dry_run=False)
    assert "Ошибка при удалении" in caplog.text


# ===== filter_components_to_delete =====
def test_filter_components_bad_last_modified(caplog):
    caplog.set_level(logging.INFO)
    comps = [
        {
            "id": "1",
            "name": "pkg",
            "version": "v1",
            "assets": [{"path": "f/file", "lastModified": "bad-date"}],
        }
    ]
    deleted = filter_components_to_delete(comps, {}, 0, 0, 0)
    assert deleted == []
    assert "ошибка парсинга lastmodified" in caplog.text.lower()


def test_filter_components_bad_last_download(caplog):
    caplog.set_level(logging.INFO)
    comps = [
        {
            "id": "1",
            "name": "pkg",
            "version": "v1",
            "assets": [
                {
                    "path": "f/file",
                    "lastModified": "2024-01-01T00:00:00Z",
                    "lastDownloaded": "not-a-date",
                }
            ],
        }
    ]
    deleted = filter_components_to_delete(comps, {}, 0, 0, 0)
    # по новой логике: no-match применяет retention=0 → артефакт удаляется
    assert len(deleted) == 1
    assert deleted[0]["name"] == "pkg"
    assert "ошибка парсинга lastdownloaded" in caplog.text.lower()



# ===== clear_repository =====
def test_clear_repository_unknown_format(monkeypatch, caplog):
    caplog.set_level(logging.WARNING)
    monkeypatch.setattr("repository.get_repository_format", lambda _: None)
    clear_repository("repoX", {})
    assert "Пропущен репозиторий" in caplog.text


def test_clear_repository_unsupported_format(monkeypatch, caplog):
    caplog.set_level(logging.WARNING)
    monkeypatch.setattr("repository.get_repository_format", lambda _: "npm")
    clear_repository("repoX", {})
    assert "неподдерживаемый формат" in caplog.text


def test_clear_repository_empty(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    monkeypatch.setattr("repository.get_repository_format", lambda _: "docker")
    monkeypatch.setattr("repository.get_repository_items", lambda *a, **k: [])
    clear_repository("repoX", {})
    assert "пуст" in caplog.text


def test_clear_repository_raw(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    monkeypatch.setattr("repository.get_repository_format", lambda _: "raw")
    monkeypatch.setattr(
        "repository.get_repository_items",
        lambda *a, **k: [{"id": "1", "path": "folder/file"}],
    )
    monkeypatch.setattr(
        "repository.convert_raw_assets_to_components",
        lambda _: [
            {
                "id": "1",
                "name": "folder",
                "version": "file",
                "assets": [{"path": "folder/file"}],
            }
        ],
    )
    monkeypatch.setattr(
        "repository.filter_components_to_delete",
        lambda *a, **k: [{"id": "1", "name": "folder", "version": "file"}],
    )
    monkeypatch.setattr("repository.delete_component", lambda *a, **k: None)
    clear_repository("repoX", {"dry_run": True})
    assert "Удаление" in caplog.text


def test_clear_repository_docker(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    monkeypatch.setattr("repository.get_repository_format", lambda _: "docker")
    monkeypatch.setattr(
        "repository.get_repository_items",
        lambda *a, **k: [{"id": "1", "name": "n", "version": "v"}],
    )
    monkeypatch.setattr(
        "repository.filter_components_to_delete",
        lambda *a, **k: [{"id": "1", "name": "n", "version": "v"}],
    )
    monkeypatch.setattr("repository.delete_component", lambda *a, **k: None)
    clear_repository("repoX", {"dry_run": True})
    assert "Удаление" in caplog.text


def test_clear_repository_maven2(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    monkeypatch.setattr("repository.get_repository_format", lambda _: "maven2")
    monkeypatch.setattr(
        "repository.get_repository_items", lambda *a, **k: [{"id": "1"}]
    )
    monkeypatch.setattr(
        "repository.filter_maven_components_to_delete",
        lambda *a, **k: [{"id": "1", "name": "n", "version": "v"}],
    )
    monkeypatch.setattr("repository.delete_component", lambda *a, **k: None)
    clear_repository("repoX", {"dry_run": True})
    assert "Удаление" in caplog.text
