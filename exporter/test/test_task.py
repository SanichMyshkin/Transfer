import pytest
from unittest.mock import patch
from metrics.repo_size import (
    extract_blob_name_from_task,
    get_task_status_for_blob,
    fetch_nexus_tasks,
    fetch_repository_sizes,
    REPO_STORAGE
)

# === Тесты extract_blob_name_from_task ===

@pytest.mark.parametrize("message,expected", [
    ("Compacting maven2 blob store", "maven2"),
    ("Deleting raw blob store temporary files", "raw"),
    ("Deleting all blob store temporary files", "all"),
    ("Some unrelated message", None),
    (None, None),
])
def test_extract_blob_name_from_task(message, expected):
    task = {"message": message}
    assert extract_blob_name_from_task(task) == expected


# === Тесты get_task_status_for_blob ===

def test_task_status_ok():
    tasks = [{"type": "blobstore.delete-temp-files", "blob_name": "raw", "lastRunResult": "OK"}]
    assert get_task_status_for_blob(tasks, "raw", "blobstore.delete-temp-files") == 1

def test_task_status_failed():
    tasks = [{"type": "blobstore.compact", "blob_name": "maven2", "lastRunResult": "FAILED"}]
    assert get_task_status_for_blob(tasks, "maven2", "blobstore.compact") == -1

def test_task_status_no_task():
    tasks = []
    assert get_task_status_for_blob(tasks, "raw", "blobstore.delete-temp-files") == 0

def test_task_status_other_blob():
    tasks = [{"type": "blobstore.compact", "blob_name": "other", "lastRunResult": "OK"}]
    assert get_task_status_for_blob(tasks, "raw", "blobstore.compact") == 0


# === Тест fetch_nexus_tasks (mocked fetch_nexus_data) ===

@patch("metrics.repo_size.fetch_nexus_data")
def test_fetch_nexus_tasks_parsing(mock_fetch):
    mock_fetch.return_value = {
        "items": [
            {"type": "blobstore.compact", "message": "Compacting maven2 blob store"},
            {"type": "blobstore.delete-temp-files", "message": "Deleting raw blob store temporary files"},
        ]
    }
    tasks = fetch_nexus_tasks("http://dummy", "/tasks", ("user", "pass"))
    assert len(tasks) == 2
    assert tasks[0]["blob_name"] == "maven2"
    assert tasks[1]["blob_name"] == "raw"


# === Тест основного пайплайна fetch_repository_sizes ===

@patch("metrics.repo_size.fetch_nexus_data")
@patch("metrics.repo_size.fetch_nexus_tasks")
@patch("metrics.repo_size.get_repository_sizes")
def test_fetch_repository_sizes_pipeline(mock_get_sizes, mock_fetch_tasks, mock_fetch_data):
    mock_fetch_data.side_effect = [
        [
            {
                "name": "my-repo",
                "type": "hosted",
                "format": "maven2",
                "storage": {"blobStoreName": "maven2"},
                "cleanup": {"policyNames": ["weekly"]}
            }
        ]
    ]
    mock_get_sizes.return_value = {"my-repo": 123456}
    mock_fetch_tasks.return_value = [
        {"type": "blobstore.compact", "blob_name": "maven2", "lastRunResult": "OK"},
        {"type": "blobstore.delete-temp-files", "blob_name": "maven2", "lastRunResult": "OK"},
    ]

    REPO_STORAGE.clear()
    fetch_repository_sizes("http://dummy", "sqlite://db", ("user", "pass"))

    samples = list(REPO_STORAGE.collect())[0].samples
    assert any(s.labels["repo_name"] == "my-repo" for s in samples)
