import os
import tempfile
import shutil
import zipfile
import tarfile
from pathlib import Path
import pandas as pd
import json


SUPPORTED_ARCHIVES = {".zip", ".tar", ".gz", ".tgz"}


def is_archive(path: Path):
    if path.suffix.lower() in SUPPORTED_ARCHIVES:
        return True
    if path.name.endswith(".tar.gz"):
        return True
    return False


def extract_archive(path: Path, to_dir: Path):
    """Извлечение одного архива"""
    suffix = path.suffix.lower()

    if suffix == ".zip":
        with zipfile.ZipFile(path, "r") as z:
            z.extractall(to_dir)
        return True

    # tar / gz / tgz
    try:
        with tarfile.open(path, "r:*") as t:
            t.extractall(to_dir)
        return True
    except:
        return False


def expand_all_archives(root_dir: Path):
    """
    Рекурсивно распаковывает ВСЕ архивы до тех пор,
    пока их не останется.
    """
    extracted = True
    while extracted:
        extracted = False

        for file in list(root_dir.rglob("*")):
            if file.is_file() and is_archive(file):
                target_dir = file.parent
                if extract_archive(file, target_dir):
                    # удаляем исходный архив
                    try:
                        file.unlink()
                    except:
                        pass
                    extracted = True


def parse_json_from_log(path: Path):
    """Извлекает JSON-объекты из обычного .log построчно."""
    rows = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                rows.append(obj)
            except json.JSONDecodeError:
                continue
    return rows


def load_all_audit_logs(archive_path):
    """
    Главная функция:
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

    try:
        # 1) первичная распаковка
        if not extract_archive(archive_path, temp_dir):
            raise SystemExit("Невозможно распаковать исходный архив.")

        # 2) рекурсивная распаковка всех найденных архивов
        expand_all_archives(temp_dir)

        # 3) ищем ВСЕ папки audit рекурсивно
        audit_dirs = list(temp_dir.rglob("audit"))
        if not audit_dirs:
            raise SystemExit("Папка audit не найдена.")

        # 4) собираем ВСЕ .log-файлы
        log_files = []
        for ad in audit_dirs:
            log_files += list(ad.rglob("*.log"))

        if not log_files:
            raise SystemExit("Не найдено ни одного *.log файла.")

        # 5) парсим все логи
        all_rows = []
        for lf in log_files:
            all_rows.extend(parse_json_from_log(lf))

        if not all_rows:
            raise SystemExit("В логах не найдено JSON-строк.")

        return pd.DataFrame(all_rows)

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
