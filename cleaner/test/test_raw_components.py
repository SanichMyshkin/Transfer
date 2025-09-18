from repository import filter_components_to_delete, convert_raw_assets_to_components


def test_raw_folder_grouping_reserved():
    comps = [
        {
            "id": "1",
            "name": "any",
            "version": "v1",
            "assets": [
                {"path": "a/b/file1.zip", "lastModified": "2024-01-01T00:00:00Z"}
            ],
        },
        {
            "id": "2",
            "name": "any",
            "version": "v2",
            "assets": [
                {"path": "a/b/file2.zip", "lastModified": "2024-02-01T00:00:00Z"}
            ],
        },
    ]
    rules = {".*": {"reserved": 1}}

    deleted = filter_components_to_delete(comps, rules, 365, 0, 0)
    assert len(deleted) == 1
    assert deleted[0]["assets"][0]["path"] == "a/b/file1.zip"


def test_raw_separate_folders():
    comps = [
        {
            "id": "1",
            "name": "pkg",
            "version": "v1",
            "assets": [
                {"path": "x/y/fileA.zip", "lastModified": "2023-01-01T00:00:00Z"}
            ],
        },
        {
            "id": "2",
            "name": "pkg",
            "version": "v2",
            "assets": [
                {"path": "x/z/fileB.zip", "lastModified": "2023-01-01T00:00:00Z"}
            ],
        },
    ]
    rules = {".*": {"retention_days": 2000, "reserved": 1}}

    # reserved срабатывает отдельно на x/y и x/z → ничего не удалится
    deleted = filter_components_to_delete(comps, rules, 100, 0, 0)
    assert len(deleted) == 0


def test_raw_retention_only():
    comps = [
        {
            "id": "1",
            "name": "pkg",
            "version": "v1",
            "assets": [
                {"path": "some/dir/old.jar", "lastModified": "2020-01-01T00:00:00Z"}
            ],
        }
    ]
    rules = {".*": {"retention_days": 100}}

    deleted = filter_components_to_delete(comps, rules, 0, 0, 0)
    assert len(deleted) == 1


def test_raw_min_days_since_last_download():
    comps = [
        {
            "id": "1",
            "name": "pkg",
            "version": "v1",
            "assets": [
                {
                    "path": "a/b/file1.txt",
                    "lastModified": "2024-01-01T00:00:00Z",
                    "lastDownloaded": "2024-12-25T00:00:00Z",
                }
            ],
        }
    ]
    rules = {".*": {"min_days_since_last_download": 10}}

    deleted = filter_components_to_delete(comps, rules, 0, 0, 0)
    assert len(deleted) == 1


def test_raw_single_file_in_root():
    assets = [
        {"id": "raw-1", "path": "fileA.zip", "lastModified": "2025-01-01T00:00:00Z"}
    ]
    components = convert_raw_assets_to_components(assets)
    # Ожидаем 0, потому что path без "/" → игнорируется
    assert len(components) == 0


def test_raw_multiple_files_same_folder():
    assets = [
        {
            "id": "1",
            "path": "libs/v1/fileA.zip",
            "lastModified": "2025-01-01T00:00:00Z",
        },
        {
            "id": "2",
            "path": "libs/v1/fileB.zip",
            "lastModified": "2025-01-02T00:00:00Z",
        },
    ]
    components = convert_raw_assets_to_components(assets)
    # Каждый файл превращается в отдельный компонент
    assert len(components) == 2
    assert components[0]["name"] == "libs/v1"
    assert components[1]["name"] == "libs/v1"


def test_raw_multiple_folders_are_separate_components():
    assets = [
        {
            "id": "1",
            "path": "folder1/file1.zip",
            "lastModified": "2025-01-01T00:00:00Z",
        },
        {
            "id": "2",
            "path": "folder2/file2.zip",
            "lastModified": "2025-01-01T00:00:00Z",
        },
    ]
    components = convert_raw_assets_to_components(assets)
    names = set(c["name"] for c in components)
    assert len(components) == 2
    assert "folder1" in names and "folder2" in names


def test_raw_deep_nested_folder_structure():
    assets = [
        {"id": "1", "path": "a/b/c/d/file.jar", "lastModified": "2025-01-01T00:00:00Z"}
    ]
    components = convert_raw_assets_to_components(assets)
    assert len(components) == 1
    assert components[0]["name"] == "a/b/c/d"
    assert components[0]["version"] == "file.jar"


def test_raw_file_without_slash_skipped():
    assets = [
        {
            "id": "1",
            "path": "",  # Пустой путь
            "lastModified": "2025-01-01T00:00:00Z",
        },
        {
            "id": "2",
            "path": "justfilename",  # Нет "/"
            "lastModified": "2025-01-01T00:00:00Z",
        },
    ]
    components = convert_raw_assets_to_components(assets)
    assert len(components) == 0
