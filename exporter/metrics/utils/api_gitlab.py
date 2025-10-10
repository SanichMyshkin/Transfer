import gitlab
import yaml
from common.logs import logging
import urllib3
import base64
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


def get_external_policies(
    gitlab_url: str,
    gitlab_token: str,
    gitlab_branch: str,
    group_id: int,
    target_path: str = "nexus/cleaner",
) -> Dict[str, str]:
    """
    Получает политики из всех проектов группы GitLab.
    Ищет YAML-файлы в пути nexus/cleaner, извлекает repo_names.

    Возвращает:
        { repo_name: ссылка_на_файл }
    """
    logging.info(f"🔗 Подключение к GitLab: {gitlab_url}")
    result = {}

    try:
        gl = get_gitlab_connection(gitlab_url, gitlab_token)
        group = gl.groups.get(group_id)
        projects = group.projects.list(all=True, include_subgroups=True)

        logging.info(f"📦 Найдено проектов в группе: {len(projects)}")

        for project_info in projects:
            try:
                project = gl.projects.get(project_info.id)
                logging.debug(f"🔍 Сканирование проекта: {project.path_with_namespace}")

                # Получаем список файлов в target_path
                items = project.repository_tree(
                    path=target_path, recursive=True, ref=gitlab_branch
                )
                yaml_files = [
                    item
                    for item in items
                    if item["type"] == "blob"
                    and item["name"].endswith((".yml", ".yaml"))
                ]

                if not yaml_files:
                    continue

                logging.info(
                    f"📁 Проект {project.path_with_namespace}: найдено {len(yaml_files)} YAML файлов"
                )

                for file_info in yaml_files:
                    file_path = file_info["path"]
                    try:
                        file_obj = project.files.get(
                            file_path=file_path, ref=gitlab_branch
                        )
                        # ✅ base64-декодирование содержимого
                        file_content = base64.b64decode(file_obj.content).decode(
                            "utf-8"
                        )

                        data = yaml.safe_load(StringIO(file_content))
                        if not isinstance(data, dict) or "repo_names" not in data:
                            continue

                        file_link = f"{gitlab_url}/{project.path_with_namespace}/-/blob/{gitlab_branch}/{file_path}"

                        for repo_name in data["repo_names"]:
                            if repo_name in result:
                                logging.warning(
                                    f"⚠️ Повтор: '{repo_name}' уже был добавлен. Файл: {file_link}"
                                )
                            else:
                                result[repo_name] = file_link

                    except Exception as e:
                        logging.error(
                            f"❌ Ошибка чтения файла {file_path} в проекте {project.path_with_namespace}: {str(e)}"
                        )

            except Exception as e:
                logging.error(
                    f"❌ Ошибка при обработке проекта {project_info.name}: {str(e)}"
                )

        logging.info(f"✅ Завершено. Собрано политик: {len(result)}")
        return result

    except Exception as e:
        logging.error(f"⛔ Критическая ошибка при получении политик: {str(e)}")
        return {}


# def get_external_policies(
#     gitlab_url: str,
#     gitlab_token: str,
#     gitlab_branch: str,
#     target_path: str = "nexus/cleaner",
# ) -> Dict[str, str]:
#     return {
#         "dckr": "https://gitlab.example.com/team/configs/-/blob/master/nexus/cleaner/policy1.yaml",
#         "docker": "https://gitlab.example.com/team/configs/-/blob/master/nexus/cleaner/policy2.yaml",
#         "nexus-repo-3": "https://gitlab.example.com/devops/cleanup/-/blob/master/nexus/cleaner/policy3.yml",
#     }


def get_gitlab_file_content(
    gitlab_url: str = None,
    gitlab_token: str = None,
    gl: gitlab.Gitlab = None,
    project_id: int = 2611,
    file_path: str = None,
    branch: str = "master",
) -> str:
    """
    Универсальная функция для получения содержимого файла из GitLab через ID проекта

    Args:
        gitlab_url: URL GitLab (если не передан gl)
        gitlab_token: Токен GitLab (если не передан gl)
        gl: Существующее подключение к GitLab
        project_id: ID проекта в GitLab
        file_path: Путь к файлу в репозитории
        branch: Ветка для получения файла

    Returns:
        Содержимое файла в виде строки
    """
    if not file_path:
        logging.error("❌ Не указан путь к файлу")
        return ""

    if not project_id:
        logging.error("❌ Не указан ID проекта")
        return ""

    try:
        # Используем существующее подключение или создаем новое
        if gl is None:
            if not gitlab_url or not gitlab_token:
                logging.error("❌ Не указаны URL или токен GitLab")
                return ""
            gl = gitlab.Gitlab(gitlab_url, private_token=gitlab_token)

        # Получаем проект по ID
        project = gl.projects.get(project_id)

        # Получаем файл
        file_data = project.files.get(file_path=file_path, ref=branch)
        file_content = file_data.decode().decode("utf-8")

        logging.info(f"✅ Получен файл {file_path} из проекта ID={project_id}")
        return file_content

    except gitlab.exceptions.GitlabGetError as e:
        if e.response_code == 404:
            logging.error(f"❌ Файл {file_path} не найден в проекте ID={project_id}")
        else:
            logging.error(f"❌ Ошибка доступа к файлу {file_path}: {str(e)}")
        return ""
    except Exception as e:
        logging.error(f"❌ Ошибка получения файла {file_path}: {str(e)}")
        return ""
