import os
import tempfile
import shutil
import zipfile
import tarfile
from pathlib import Path
import pandas as pd
import json
import logging


logger = logging.getLogger("audit_loader")


SUPPORTED_ARCHIVES = {".zip", ".tar", ".gz", ".tgz"}


def is_archive(path: Path):
    if path.suffix.lower() in SUPPORTED_ARCHIVES:
        return True
    if path.name.endswith(".tar.gz"):
        return True
    return False


def extract_archive(path: Path, to_dir: Path):
    """Извлечение одного архива"""
    logger.info(f"Распаковка архива: {path}")

    suffix = path.suffix.lower()

    if suffix == ".zip":
        with zipfile.ZipFile(path, "r") as z:
            z.extractall(to_dir)
        return True

    try:
        with tarfile.open(path, "r:*") as t:
            t.extractall(to_dir)
        return True
    except:
        logger.warning(f"Не удалось распаковать архив: {path}")
        return False


def expand_all_archives(root_dir: Path):
    """
    Рекурсивно распаковывает ВСЕ архивы внутри root_dir
    """
    logger.info("Начинаем рекурсивную распаковку вложенных архивов...")

    extracted = True
    while extracted:
        extracted = False

        for file in list(root_dir.rglob("*")):
            if file.is_file() and is_archive(file):
                logger.info(f"Найден вложенный архив: {file}")

                target_dir = file.parent

                if extract_archive(file, target_dir):
                    logger.debug(f"Удаляем архив после распаковки: {file}")
                    try:
                        file.unlink()
                    except:
                        pass
                    extracted = True

    logger.info("Рекурсивная распаковка завершена.")


def parse_json_from_log(path: Path):
    """Извлекает JSON-объекты из обычного .log построчно."""
    logger.info(f"Чтение лог-файла: {path}")

    rows = []
    count = 0

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                rows.append(obj)
                count += 1
            except json.JSONDecodeError:
                continue

    logger.info(f" -> JSON-строк извлечено: {count}")
    return rows


def load_all_audit_logs(archive_path):
    """
    - разархивирует переданный архив
    - рекурсивно распаковывает все вложенные архивы
    - ищет ВСЕ папки audit
    - собирает ВСЕ файлы .log
    - вытаскивает из них JSON-строки
    """
    archive_path = Path(archive_path)
    if not archive_path.exists():
        raise FileNotFoundError(f"Архив не найден: {archive_path}")

    temp_dir = Path(tempfile.mkdtemp(prefix="audit_extract_"))
    logger.info(f"Создана временная директория: {temp_dir}")

    try:
        logger.info(f"Начальная распаковка: {archive_path}")

        # 1) первичная распаковка
        if not extract_archive(archive_path, temp_dir):
            raise SystemExit("Невозможно распаковать исходный архив.")

        # 2) рекурсивная распаковка вложенных архивов
        expand_all_archives(temp_dir)

        # 3) поиск папок audit
        audit_dirs = list(temp_dir.rglob("audit"))
        logger.info(f"Найдено папок audit: {len(audit_dirs)}")

        if not audit_dirs:
            raise SystemExit("Папка audit не найдена.")

        for ad in audit_dirs:
            logger.debug(f" -> audit: {ad}")

        # 4) поиск .log файлов
        log_files = []
        for ad in audit_dirs:
            found = list(ad.rglob("*.log"))
            logger.info(f"В {ad} найдено .log файлов: {len(found)}")

            for lf in found:
                logger.debug(f" -> log: {lf}")

            log_files += found

        if not log_files:
            raise SystemExit("Нет .log файлов.")

        # 5) читаем все логи
        all_rows = []
        for lf in log_files:
            all_rows.extend(parse_json_from_log(lf))

        logger.info(f"Всего JSON-строк во всех логах: {len(all_rows)}")

        if not all_rows:
            raise SystemExit("В логах нет JSON-строк.")

        return pd.DataFrame(all_rows)

    finally:
        logger.info(f"Удаляем временную директорию: {temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)
