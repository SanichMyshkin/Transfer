import logging
import main


def test_main_no_configs(tmp_path, caplog, monkeypatch):
    monkeypatch.setattr(main, "__file__", str(tmp_path / "main.py"))
    caplog.set_level(logging.WARNING)

    main.main()
    assert "нет YAML-файлов" in caplog.text


def test_main_with_valid_config(tmp_path, caplog, monkeypatch):
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    cfg_file = config_dir / "test.yaml"
    cfg_file.write_text("repo_names:\n  - test-repo\n", encoding="utf-8")

    monkeypatch.setattr(main, "__file__", str(tmp_path / "main.py"))

    called = {}

    def fake_load_config(path):
        called["load"] = path
        return {"repo_names": ["test-repo"]}

    def fake_clear_repository(repo, config):
        called["clear"] = (repo, config)

    monkeypatch.setattr(main, "load_config", fake_load_config)
    monkeypatch.setattr(main, "clear_repository", fake_clear_repository)

    caplog.set_level(logging.INFO)  # <--- ДОБАВИЛ

    main.main()

    assert "Обработка файла конфигурации" in caplog.text
    assert "load" in called
    assert "clear" in called
    assert called["clear"][0] == "test-repo"


def test_main_with_invalid_config(tmp_path, caplog, monkeypatch):
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    cfg_file = config_dir / "bad.yaml"
    cfg_file.write_text("repo_names: [", encoding="utf-8")

    monkeypatch.setattr(main, "__file__", str(tmp_path / "main.py"))

    def fake_load_config(path):
        return None

    def fake_clear_repository(repo, config):
        raise AssertionError("Не должно вызываться")

    monkeypatch.setattr(main, "load_config", fake_load_config)
    monkeypatch.setattr(main, "clear_repository", fake_clear_repository)

    caplog.set_level(logging.INFO)  # <--- ДОБАВИЛ

    main.main()

    assert "Обработка файла конфигурации" in caplog.text
