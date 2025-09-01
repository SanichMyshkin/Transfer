import os
import logging
from common import load_config
from repository import clear_repository


def main():
    config_dir = os.path.join(os.path.dirname(__file__), "configs")
    config_files = []

    for root, _, files in os.walk(config_dir):
        for f in files:
            if f.endswith(".yaml") or f.endswith(".yml"):
                config_files.append(os.path.join(root, f))

    if not config_files:
        logging.warning("[MAIN] ⚠️ В папке 'configs/' и подкаталогах нет YAML-файлов")
        return

    for cfg_path in config_files:
        logging.info(f"\n📄 Обработка файла конфигурации: {cfg_path}")
        config = load_config(cfg_path)
        if not config:
            continue
        repos = config.get("repo_names", [])
        for repo in repos:
            clear_repository(repo, config)


if __name__ == "__main__":
    main()
