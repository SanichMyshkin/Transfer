import gitlab
import yaml
import logging
from io import StringIO
from config import GITLAB_URL, GITLAB_TOKEN, GITLAB_BRANCH

# ───── Логирование ───── #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ───── Константы ───── #
TARGET_PATH = "nexus/nexus-cleaner"

# ───── Подключение к GitLab ───── #
gl = gitlab.Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN)
gl.auth()

result = {}           # {'repo_name': 'gitlab_url'}
repo_sources = {}     # {'repo_name': [file1, file2]}
files_processed = 0
repos_found = 0

logger.info(f"🔗 Подключено к GitLab: {GITLAB_URL}")
logger.info("🔍 Начинаем обход проектов...")

# ───── Обход проектов ───── #
projects = gl.projects.list(all=True)

for project in projects:
    try:
        items = project.repository_tree(path=TARGET_PATH, recursive=True)
        yaml_files = [
            item for item in items
            if item['type'] == 'blob' and item['name'].endswith(('.yml', '.yaml'))
        ]

        if not yaml_files:
            continue

        logger.info(f"📁 Проект {project.path_with_namespace}: найдено {len(yaml_files)} yaml-файлов")

        for file in yaml_files:
            file_path = file['path']
            try:
                f = project.files.get(file_path=file_path, ref=GITLAB_BRANCH)
                content = f.decode().decode('utf-8')
                data = yaml.safe_load(StringIO(content))
                files_processed += 1

                if isinstance(data, dict) and 'repo_names' in data:
                    for repo_name in data['repo_names']:
                        link = f"{GITLAB_URL}/{project.path_with_namespace}/-/blob/{GITLAB_BRANCH}/{file_path}"

                        if repo_name in result:
                            logger.warning(
                                f"⚠️ Повтор: '{repo_name}' найден в нескольких конфигурациях:\n"
                                f"    - уже был: {repo_sources[repo_name][-1]}\n"
                                f"    - сейчас: {link}"
                            )

                        result[repo_name] = link
                        repo_sources.setdefault(repo_name, []).append(link)
                        repos_found += 1

            except Exception as e:
                logger.error(f"❌ Ошибка чтения {file_path} в {project.path_with_namespace}: {e}")

    except gitlab.exceptions.GitlabGetError:
        logger.info(f"⏭️ Пропуск {project.path_with_namespace}: путь '{TARGET_PATH}' не найден.")
        continue

# ───── Финальный отчёт ───── #
logger.info("✅ Обработка завершена.")
logger.info(f"📄 Всего yaml-файлов обработано: {files_processed}")
logger.info(f"📦 Уникальных repo_names найдено: {len(result)}")
logger.info(f"🔁 Всего вхождений repo_names (включая повторы): {repos_found}")

# ───── Вывод результата ───── #
print("\nРезультат:")
for repo, link in result.items():
    print(f"{repo}: {link}")
