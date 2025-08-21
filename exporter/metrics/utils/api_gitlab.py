import gitlab
import yaml
from common.logs import logging
import urllib3
from io import StringIO
from typing import Dict

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_gitlab_connection(gitlab_url: str, gitlab_token: str) -> gitlab.Gitlab:
    """Создание подключения к GitLab"""
    gl = gitlab.Gitlab(gitlab_url, private_token=gitlab_token, ssl_verify=False)
    gl.auth()
    return gl


def process_yaml_file(
    project, file_info: Dict, branch: str, result: Dict, gitlab_url: str
) -> bool:
    """Обработка одного YAML файла"""
    file_path = file_info["path"]
    try:
        file_content = (
            project.files.get(file_path=file_path, ref=branch).decode().decode("utf-8")
        )
        data = yaml.safe_load(StringIO(file_content))

        if not isinstance(data, dict) or "repo_names" not in data:
            return False

        file_link = (
            f"{gitlab_url}/{project.path_with_namespace}/-/blob/{branch}/{file_path}"
        )

        for repo_name in data["repo_names"]:
            if repo_name in result:
                logging.warning(
                    f"⚠️ Повтор: '{repo_name}' уже был добавлен. Файл: {file_link}"
                )
            else:
                result[repo_name] = file_link
        return True

    except Exception as e:
        logging.error(
            f"❌ Ошибка в файле {file_path} ({project.path_with_namespace}): {str(e)}"
        )
        return False


def scan_project_for_policies(
    project, branch: str, target_path: str, gitlab_url: str
) -> Dict:
    """Сканирование одного проекта на наличие политик"""
    result = {}
    try:
        items = project.repository_tree(path=target_path, recursive=True, ref=branch)
        yaml_files = [
            item
            for item in items
            if item["type"] == "blob" and item["name"].endswith((".yml", ".yaml"))
        ]

        if not yaml_files:
            logging.debug(f"⏭️ Пропуск {project.path_with_namespace}: нет YAML файлов")
            return result

        logging.info(
            f"📁 Проект {project.path_with_namespace}: найдено {len(yaml_files)} файлов"
        )

        for file in yaml_files:
            if process_yaml_file(project, file, branch, result, gitlab_url):
                logging.debug(f"✅ Обработан: {file['path']}")

    except gitlab.exceptions.GitlabGetError:
        logging.info(f"⏭️ Пропуск {project.path_with_namespace}: путь не найден")
    except Exception as e:
        logging.error(f"❌ Ошибка проекта {project.path_with_namespace}: {str(e)}")

    return result


# def get_external_policies(
#     gitlab_url: str,
#     gitlab_token: str,
#     gitlab_branch: str,
#     target_path: str = "nexus/cleaner",
# ) -> Dict[str, str]:
#     """Основная функция для получения внешних политик (сохранен оригинальный интерфейс)"""
#     logging.info(f"🔗 Подключение к GitLab: {gitlab_url}")

#     final_result = {}
#     total_files = 0
#     total_repos = 0

#     try:
#         gl = get_gitlab_connection(gitlab_url, gitlab_token)
#         logging.info("🔍 Начинаем сканирование проектов...")

#         for project in gl.projects.list(all=True, iterator=True):
#             project_result = scan_project_for_policies(
#                 project, gitlab_branch, target_path, gitlab_url
#             )
#             total_files += len(project_result)
#             total_repos += sum(1 for repo in project_result if repo not in final_result)
#             final_result.update(project_result)

#         logging.info(
#             f"✅ Завершено. Обработано файлов: {total_files}, уникальных репозиториев: {len(final_result)}"
#         )
#         return final_result

#     except Exception as e:
#         logging.error(f"⛔ Критическая ошибка: {str(e)}")
#         raise


def get_external_policies(
    gitlab_url: str,
    gitlab_token: str,
    gitlab_branch: str,
    target_path: str = "nexus/cleaner",
) -> Dict[str, str]:
    return {
        "dckr": "https://gitlab.example.com/team/configs/-/blob/master/nexus/cleaner/policy1.yaml",
        "docker": "https://gitlab.example.com/team/configs/-/blob/master/nexus/cleaner/policy2.yaml",
        "nexus-repo-3": "https://gitlab.example.com/devops/cleanup/-/blob/master/nexus/cleaner/policy3.yml",
    }


def get_gitlab_file_content(
    gitlab_url: str = None,
    gitlab_token: str = None,
    gl: gitlab.Gitlab = None,
    project_path: str = "sre-platfom-support/nexus-15562",
    file_path: str = None,
    branch: str = "master",
) -> str:
    """
    Универсальная функция для получения содержимого файла из GitLab

    Args:
        gitlab_url: URL GitLab (если не передан gl)
        gitlab_token: Токен GitLab (если не передан gl)
        gl: Существующее подключение к GitLab
        project_path: Путь к проекту (namespace/project)
        file_path: Путь к файлу в репозитории
        branch: Ветка для получения файла

    Returns:
        Содержимое файла в виде строки
    """
    if not file_path:
        logging.error("❌ Не указан путь к файлу")
        return ""

    try:
        # Используем существующее подключение или создаем новое
        if gl is None:
            if not gitlab_url or not gitlab_token:
                logging.error("❌ Не указаны URL или токен GitLab")
                return ""
            gl = get_gitlab_connection(gitlab_url, gitlab_token)

        # Получаем проект и файл
        project = gl.projects.get(project_path)
        file_content = (
            project.files.get(file_path=file_path, ref=branch).decode().decode("utf-8")
        )

        logging.info(f"✅ Получен файл {file_path} из проекта {project_path}")
        return file_content

    except gitlab.exceptions.GitlabGetError as e:
        if e.response_code == 404:
            logging.error(f"❌ Файл {file_path} не найден в проекте {project_path}")
        else:
            logging.error(f"❌ Ошибка доступа к файлу {file_path}: {str(e)}")
        return ""
    except Exception as e:
        logging.error(f"❌ Ошибка получения файла {file_path}: {str(e)}")
        return ""
