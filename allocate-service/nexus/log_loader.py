import os
import shutil
import gzip
import tarfile
import zipfile
import logging
from pathlib import Path
import pandas as pd
import json
import pyarrow as pa
import pyarrow.parquet as pq


logger = logging.getLogger("audit_loader")


# ============================================================
# Архиваторы
# ============================================================

def extract_zip(path: Path, to_dir: Path):
    try:
        with zipfile.ZipFile(path, "r") as z:
            z.extractall(to_dir)
        logger.info(f"Распакован ZIP: {path}")
        return True
    except Exception as e:
        logger.error(f"Не удалось распаковать ZIP {path}: {e}")
        return False


def extract_tar(path: Path, to_dir: Path):
    try:
        with tarfile.open(path, "r:*") as t:
            t.extractall(to_dir)
        logger.info(f"Распакован TAR: {path}")
        return True
    except Exception as e:
        logger.error(f"Не удалось распаковать TAR {path}: {e}")
        return False


def extract_gzip_log(path: Path):
    """*.log.gz → *.log"""
    try:
        out_path = path.with_suffix("")
        logger.info(f"Распаковка GZIP: {path} → {out_path}")
        with gzip.open(path, "rb") as src, open(out_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        return out_path
    except Exception as e:
        logger.error(f"Ошибка распаковки gzip {path}: {e}")
        return None


def expand_archives(root: Path):
    """
    Рекурсивно распаковываем ВСЕ архивы:
    zip, tar, tar.gz, tgz, log.gz
    """
    logger.info("Начата рекурсивная распаковка архивов")

    extracted = True
    while extracted:
        extracted = False

        for file in list(root.rglob("*")):
            if not file.is_file():
                continue

            name = file.name.lower()

            if name.endswith(".zip"):
                if extract_zip(file, file.parent):
                    file.unlink()
                    extracted = True
                continue

            if name.endswith(".tar") or name.endswith(".tar.gz") or name.endswith(".tgz"):
                if extract_tar(file, file.parent):
                    file.unlink()
                    extracted = True
                continue

            if name.endswith(".log.gz"):
                new_file = extract_gzip_log(file)
                if new_file:
                    file.unlink()
                    extracted = True
                continue

    logger.info("Все архивы успешно развёрнуты")


# ============================================================
# Стриминговый парсинг JSON
# ============================================================

def safe_parse_json(line: str):
    """
    Безопасный парс JSON.
    Пропускаем строки без {} и слишком длинные.
    """
    line = line.strip()

    if "{" not in line or "}" not in line:
        return None

    if len(line) > 5_000_000:  # >5MB
        return None

    try:
        return json.loads(line)
    except Exception:
        return None


def write_batch(batch, writer, fields):
    """
    Выравнивание схемы и запись батча в Parquet.
    """
    aligned = []
    for obj in batch:
        row = {field: obj.get(field, None) for field in fields}
        aligned.append(row)

    table = pa.Table.from_pylist(aligned)
    writer.write_table(table)


def stream_log_to_parquet(path: Path, writer, fields: set):
    logger.info(f"Чтение лог-файла: {path}")

    batch = []
    batch_size = 10000
    total = 0

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            obj = safe_parse_json(raw)
            if obj is None:
                continue

            # дополняем схему новыми ключами
            for k in obj:
                if k not in fields:
                    fields.add(k)

            batch.append(obj)
            total += 1

            if len(batch) >= batch_size:
                write_batch(batch, writer, fields)
                batch.clear()

    if batch:
        write_batch(batch, writer, fields)

    logger.info(f" → JSON записей извлечено: {total}")


# ============================================================
# Основной процесс загрузки логов
# ============================================================

def load_all_audit_logs(archive_path: str) -> pd.DataFrame:
    """
    Полная потоковая обработка:
      - распаковка
      - поиск audit/
      - обработка всех .log
      - запись JSON → Parquet
      - чтение Parquet → DataFrame
    """
    root = Path("temp_extract")
    if root.exists():
        shutil.rmtree(root)
    root.mkdir()

    parsed_dir = root / "parsed"
    parsed_dir.mkdir()

    parquet_file = parsed_dir / "records.parquet"

    archive_path = Path(archive_path)
    logger.info(f"Начинаем обработку архива: {archive_path}")
    logger.info(f"Рабочая директория: {root}")

    # первичная распаковка главного архива
    if not extract_zip(archive_path, root) and \
       not extract_tar(archive_path, root):
        raise Exception("Не удалось распаковать входной архив")

    # рекурсивная распаковка
    expand_archives(root)

    # поиск папок audit
    audit_dirs = list(root.rglob("audit"))
    if not audit_dirs:
        raise Exception("Папка audit не найдена")

    for a in audit_dirs:
        logger.info(f"Найдена audit: {a}")

    # сбор всех логов
    log_files = []
    for a in audit_dirs:
        found = list(a.rglob("*.log"))
        for lf in found:
            logger.info(f"Найден лог: {lf}")
        log_files += found

    if not log_files:
        raise Exception("Нет .log файлов")

    # создаём Parquet writer
    logger.info("Создаём Parquet writer")
    writer = pq.ParquetWriter(
        parquet_file,
        schema=None,
        use_dictionary=True,
        compression="snappy"
    )

    schema_fields = set()

    # потоковая обработка логов
    logger.info("Старт потоковой обработки логов…")
    for lf in log_files:
        stream_log_to_parquet(lf, writer, schema_fields)

    writer.close()

    logger.info(f"Выгрузка завершена. Чтение Parquet: {parquet_file}")

    # загружаем DataFrame
    df = pd.read_parquet(parquet_file)

    logger.info(f"Всего JSON строк в DataFrame: {len(df)}")

    return df
