import gitlab
import yaml
from common.logs import logging
import urllib3
from io import StringIO
from typing import Dict


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_external_policies(
    gitlab_url: str,
    gitlab_token: str,
    gitlab_branch: str,
    target_path: str = "nexus/cleaner",
) -> Dict[str, str]:  # Теперь возвращает только один словарь
    result = {}  # {'repo_name': 'gitlab_url'}
    files_processed = 0
    repos_found = 0

    try:
        # Подключение к GitLab
        gl = gitlab.Gitlab(gitlab_url, private_token=gitlab_token, ssl_verify=False)
        gl.auth()
        logging.info(f"🔗 Подключено к GitLab: {gitlab_url}")
        logging.info("🔍 Начинаем обход проектов...")

        # Обход проектов
        projects = gl.projects.list(all=True)

        for project in projects:
            try:
                items = project.repository_tree(path=target_path, recursive=True)
                yaml_files = [
                    item
                    for item in items
                    if item["type"] == "blob"
                    and item["name"].endswith((".yml", ".yaml"))
                ]

                if not yaml_files:
                    continue

                logging.info(
                    f"📁 Проект {project.path_with_namespace}: найдено {len(yaml_files)} yaml-файлов"
                )

                for file in yaml_files:
                    file_path = file["path"]
                    try:
                        f = project.files.get(file_path=file_path, ref=gitlab_branch)
                        content = f.decode().decode("utf-8")
                        data = yaml.safe_load(StringIO(content))
                        files_processed += 1

                        if isinstance(data, dict) and "repo_names" in data:
                            for repo_name in data["repo_names"]:
                                link = f"{gitlab_url}/{project.path_with_namespace}/-/blob/{gitlab_branch}/{file_path}"

                                if repo_name in result:
                                    logging.warning(
                                        f"⚠️ Повтор: '{repo_name}' уже был добавлен ранее. "
                                        f"Новый файл: {link}"
                                    )
                                else:
                                    result[repo_name] = link
                                    repos_found += 1

                    except Exception as e:
                        logging.error(
                            f"❌ Ошибка чтения {file_path} в {project.path_with_namespace}: {e}"
                        )

            except gitlab.exceptions.GitlabGetError:
                logging.info(
                    f"⏭️ Пропуск {project.path_with_namespace}: путь '{target_path}' не найден."
                )
                continue

        # Финальный отчёт
        logging.info("✅ Обработка завершена.")
        logging.info(f"📄 Всего yaml-файлов обработано: {files_processed}")
        logging.info(f"📦 Уникальных repo_names найдено: {len(result)}")

        return result  # Возвращаем только один словарь

    except Exception as e:
        logging.error(f"⛔ Критическая ошибка при работе с GitLab: {e}")
        raise


#### ЗАГЛУШКИ
def get_file_raw_ports() -> str:
    """Заглушка: возвращает текст файла с docker run"""
    return """"""


def get_nginx() -> str:
    """Заглушка: возвращает текст nginx конфигурации"""
    return """"""