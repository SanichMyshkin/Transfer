import os
import tempfile
import shutil
import zipfile
import tarfile
from pathlib import Path
import pandas as pd
import json


def _extract_if_archive(src_path, dst_dir):
    """Распаковка zip/tar.gz/tgz/7z и т.д. в dst_dir"""
    suffix = src_path.suffix.lower()

    # zip
    if suffix == ".zip":
        with zipfile.ZipFile(src_path, "r") as z:
            z.extractall(dst_dir)
        return True

    # tar / tar.gz / tgz
    if suffix in {".tar", ".gz", ".tgz"} or src_path.name.endswith(".tar.gz"):
        try:
            with tarfile.open(src_path, "r:*") as t:
                t.extractall(dst_dir)
            return True
        except tarfile.TarError:
            return False

    return False


def _recursive_extract(root, work_dir):
    """
    Находит ВСЕ архивы в work_dir, извлекает, пока архивов не останется.
    """
    extracted = True
    while extracted:
        extracted = False
        for path in list(Path(work_dir).rglob("*")):
            if path.is_file():
                if _extract_if_archive(path, path.parent):
                    # удаляем сам архив, он больше не нужен
                    try:
                        path.unlink()
                    except:
                        pass
                    extracted = True


def _load_jsonl(fname):
    """Читает JSONL безопасно"""
    rows = []
    with open(fname, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


def load_all_audit_logs(archive_path):
    """
    Главная функция:
    - распаковывает архив любого уровня вложенности
    - ищет папку audit
    - собирает все *.jsonl файлы
    - возвращает единый DataFrame (сырые строки)
    """
    archive_path = Path(archive_path)
    if not archive_path.exists():
        raise FileNotFoundError(f"Не найден архив: {archive_path}")

    tmpdir = tempfile.mkdtemp(prefix="audit_extract_")

    try:
        # 1) распаковываем верхний архив в tmpdir
        if not _extract_if_archive(archive_path, tmpdir):
            raise ValueError(f"Формат архива не поддерживается: {archive_path}")

        # 2) рекурсивно распаковываем всё, что есть внутри
        _recursive_extract(archive_path, tmpdir)

        # 3) ищем папку audit любой глубины
        audits = list(Path(tmpdir).rglob("audit"))
        if not audits:
            raise SystemExit("Папка audit внутри архива не найдена.")

        # 4) собираем JSONL-файлы
        log_files = []
        for audit_dir in audits:
            log_files += list(audit_dir.rglob("*.jsonl"))

        if not log_files:
            raise SystemExit("В папке audit нет JSONL-файлов.")

        # 5) читаем все логи в общий список
        all_rows = []
        for lf in log_files:
            all_rows.extend(_load_jsonl(lf))

        if not all_rows:
            raise SystemExit("JSONL-файлы пусты.")

        return pd.DataFrame(all_rows)

    finally:
        # можно оставить архив после дебага — закомментируй строку ниже
        shutil.rmtree(tmpdir, ignore_errors=True)
