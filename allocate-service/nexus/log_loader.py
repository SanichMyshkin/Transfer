import os
import tempfile
import shutil
import zipfile
import tarfile
from pathlib import Path
import pandas as pd
import json


def _extract_if_archive(src_path, dst_dir):
    """Распаковка zip/tar.gz/tgz."""
    suffix = src_path.suffix.lower()

    if suffix == ".zip":
        with zipfile.ZipFile(src_path, "r") as z:
            z.extractall(dst_dir)
        return True

    if suffix in {".tar", ".gz", ".tgz"} or src_path.name.endswith(".tar.gz"):
        try:
            with tarfile.open(src_path, "r:*") as t:
                t.extractall(dst_dir)
            return True
        except:
            return False

    return False


def _recursive_extract(work_dir):
    """
    Извлекает ВСЕ архивы внутри work_dir до тех пор,
    пока архивов не останется.
    """
    extracted = True
    while extracted:
        extracted = False
        for path in list(Path(work_dir).rglob("*")):
            if path.is_file():
                if _extract_if_archive(path, path.parent):
                    try:
                        path.unlink()
                    except:
                        pass
                    extracted = True


def _load_json_from_log(fname):
    """
    Читает обычный .log, в каждой строке ищет JSON.
    Некорректные строки пропускаются.
    """
    rows = []
    with open(fname, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # пытаемся распарсить json внутри строки
            try:
                obj = json.loads(line)
                rows.append(obj)
            except json.JSONDecodeError:
                # строка не JSON — пропускаем
                continue
    return rows


def load_all_audit_logs(archive_path):
    """
    - распаковывает архив любой вложенности
    - ищет папку audit
    - собирает ВСЕ *.log файлы
    - вытаскивает из них строки, содержащие JSON
    - формирует единый DataFrame
    """
    archive_path = Path(archive_path)
    if not archive_path.exists():
        raise FileNotFoundError(f"Не найден архив: {archive_path}")

    tmpdir = tempfile.mkdtemp(prefix="audit_extract_")

    try:
        # 1) первичная распаковка
        if not _extract_if_archive(archive_path, tmpdir):
            raise SystemExit("Не удалось распаковать архив.")

        # 2) рекурсивная распаковка
        _recursive_extract(tmpdir)

        # 3) ищем ВСЕ директории audit
        audits = list(Path(tmpdir).rglob("audit"))
        if not audits:
            raise SystemExit("Папка audit не найдена в архиве.")

        # 4) собираем все *.log
        log_files = []
        for audit_dir in audits:
            log_files += list(audit_dir.rglob("*.log"))

        if not log_files:
            raise SystemExit("В папке audit нет .log файлов.")

        # 5) читаем все строки из всех логов
        all_rows = []
        for lf in log_files:
            all_rows.extend(_load_json_from_log(lf))

        if not all_rows:
            raise SystemExit("В .log-файлах нет JSON-строк.")

        return pd.DataFrame(all_rows)

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
