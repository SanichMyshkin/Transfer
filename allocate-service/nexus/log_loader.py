import os
import shutil
import gzip
import tarfile
import zipfile
import logging
from pathlib import Path
import pandas as pd
import json


logger = logging.getLogger("audit_loader")


def extract_zip(path: Path, to_dir: Path):
    try:
        with zipfile.ZipFile(path, "r") as z:
            z.extractall(to_dir)
        return True
    except:
        return False


def extract_tar(path: Path, to_dir: Path):
    try:
        with tarfile.open(path, "r:*") as t:
            t.extractall(to_dir)
        return True
    except:
        return False


def extract_gzip_log(path: Path):
    """
    Распаковка audit-2025-09-14.log.gz → audit-2025-09-14.log
    """
    logger.info(f"Распаковка gzip-файла: {path}")

    out_path = path.with_suffix("")  # удаляем .gz
    try:
        with gzip.open(path, "rb") as gz_in:
            with open(out_path, "wb") as f_out:
                shutil.copyfileobj(gz_in, f_out)
        return out_path
    except Exception as e:
        logger.error(f"Ошибка при распаковке gzip: {e}")
        return None


def expand_archives(work_dir: Path):
    """
    Распаковка ВСЕХ zip/tar/gz внутри рабочей директории.
    """
    extracted_any = True
    while extracted_any:
        extracted_any = False

        for file in list(work_dir.rglob("*")):
            if not file.is_file():
                continue

            name = file.name.lower()

            # ZIP
            if name.endswith(".zip"):
                logger.info(f"Найден zip: {file}")
                if extract_zip(file, file.parent):
                    file.unlink()
                    extracted_any = True
                continue

            # TAR or TAR.GZ
            if name.endswith(".tar") or name.endswith(".tar.gz") or name.endswith(".tgz"):
                logger.info(f"Найден tar: {file}")
                if extract_tar(file, file.parent):
                    file.unlink()
                    extracted_any = True
                continue

            # GZIP LOG → .log.gz
            if name.endswith(".log.gz"):
                new_file = extract_gzip_log(file)
                if new_file is not None:
                    file.unlink()
                    extracted_any = True
                continue


def parse_json_from_log(path: Path):
    logger.info(f"Чтение лог-файла: {path}")

    rows = []
    cnt = 0

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            try:
                obj = json.loads(line)
                rows.append(obj)
                cnt += 1
            except json.JSONDecodeError:
                continue

    logger.info(f" -> JSON-строк извлечено: {cnt}")
    return rows


def load_all_audit_logs(archive_path: str):
    project_temp = Path("temp_extract")
    project_temp.mkdir(exist_ok=True)

    logger.info(f"Рабочая директория: {project_temp}")

    archive_path = Path(archive_path)

    if not extract_zip(archive_path, project_temp) and \
       not extract_tar(archive_path, project_temp):
        raise Exception("Не удалось распаковать главный архив")

    logger.info("Первичная распаковка завершена. Начинаем рекурсивную...")

    expand_archives(project_temp)

    logger.info("Поиск папок audit...")
    audit_dirs = list(project_temp.rglob("audit"))
    for d in audit_dirs:
        logger.info(f"Найдена audit: {d}")

    if not audit_dirs:
        raise Exception("Папки audit не найдены")

    logger.info("Поиск .log файлов...")
    log_files = []
    for d in audit_dirs:
        files = list(d.rglob("*.log"))
        for f in files:
            logger.info(f"Найден лог-файл: {f}")
        log_files.extend(files)

    if not log_files:
        raise Exception(" *.log файлов нет")

    all_rows = []
    for lf in log_files:
        all_rows.extend(parse_json_from_log(lf))

    logger.info(f"Всего JSON-строк собрано: {len(all_rows)}")

    return pd.DataFrame(all_rows)
