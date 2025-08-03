import pytest
from metrics.docker_tags import process_docker_result

@pytest.fixture
def sample_docker_data():
    return [
        # Один и тот же образ в разных репозиториях
        ('library/redis', '7.2', 'docker-proxy', 'docker', 'default-blob'),
        ('library/redis', '7.4', 'docker-proxy', 'docker', 'default-blob'),
        ('library/redis', 'latest', 'docker-proxy', 'docker', 'default-blob'),

        ('library/redis', '7.4', 'docker-test-repo', 'docker', 'test-blob'),
        ('library/redis', 'latest', 'docker-test-repo', 'docker', 'test-blob'),

        # Другой образ
        ('library/nginx', '1.25', 'docker-proxy', 'docker', 'default-blob'),
        ('library/nginx', 'latest', 'docker-proxy', 'docker', 'default-blob'),
    ]


def test_process_docker_result_grouping(sample_docker_data):
    result = process_docker_result(sample_docker_data)

    assert isinstance(result, list)
    assert len(result) == 3  # redis@docker-proxy, redis@docker-test-repo, nginx@docker-proxy

    redis_main = next((r for r in result if r['image'] == 'library/redis' and r['repoName'] == 'docker-proxy'), None)
    redis_test = next((r for r in result if r['image'] == 'library/redis' and r['repoName'] == 'docker-test-repo'), None)
    nginx = next((r for r in result if r['image'] == 'library/nginx'), None)

    # Проверка тегов
    assert sorted(redis_main['tags']) == ['7.2', '7.4', 'latest']
    assert sorted(redis_test['tags']) == ['7.4', 'latest']
    assert sorted(nginx['tags']) == ['1.25', 'latest']

    # Проверка репозиториев
    assert redis_main['repoFormat'] == 'docker'
    assert redis_main['blobName'] == 'default-blob'

    assert redis_test['repoFormat'] == 'docker'
    assert redis_test['blobName'] == 'test-blob'


def test_process_docker_result_deduplication():
    input_data = [
        ('library/python', '3.11', 'repo1', 'docker', 'blob1'),
        ('library/python', '3.11', 'repo1', 'docker', 'blob1'),  # Дубликат
    ]
    result = process_docker_result(input_data)

    assert len(result) == 1
    assert result[0]['image'] == 'library/python'
    assert result[0]['repoName'] == 'repo1'
    assert result[0]['tags'] == ['3.11']


def test_process_docker_result_empty():
    result = process_docker_result([])
    assert result == []
