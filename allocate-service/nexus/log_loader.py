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
# Распаковка архивов
# ============================================================

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
    out_path = path.with_suffix("")  # .log.gz → .log
    try:
        logger.info(f"Распаковка gzip: {path}")
        with gzip.open(path, "rb") as gz_in:
            with open(out_path, "wb") as f_out:
                shutil.copyfileobj(gz_in, f_out)
        return out_path
    except Exception as e:
        logger.error(f"Ошибка gzip: {path} → {e}")
        return None


def expand_archives(work_dir: Path):
    """
    Распаковывает ВСЕ архивы до тех пор,
    пока они встречаются.
    """
    extracted_any = True
    while extracted_any:
        extracted_any = False

        for file in list(work_dir.rglob("*")):
            if not file.is_file():
                continue

            name = file.name.lower()

            if name.endswith(".zip"):
                logger.info(f"ZIP найден: {file}")
                if extract_zip(file, file.parent):
                    file.unlink()
                    extracted_any = True
                continue

            if name.endswith(".tar") or name.endswith(".tar.gz") or name.endswith(".tgz"):
                logger.info(f"TAR найден: {file}")
                if extract_tar(file, file.parent):
                    file.unlink()
                    extracted_any = True
                continue

            if name.endswith(".log.gz"):
                new_file = extract_gzip_log(file)
                if new_file:
                    file.unlink()
                    extracted_any = True
                continue


# ============================================================
# Стриминговый парсер JSON → запись в Parquet
# ============================================================

def safe_parse_json(line: str):
    """Безопасный парс JSON."""
    line = line.strip()

    # быстрый тест на JSON
    if "{" not in line or "}" not in line:
        return None

    # ограничение на размер строки
    if len(line) > 5_000_000:  # 5MB
        return None

    try:
        return json.loads(line)
    except Exception:
        return None


def stream_log_to_parquet(path: Path, writer):
    """
    Читает лог-файл построчно и добавляет JSON записи в Parquet writer.
    """
    logger.info(f"Чтение лога: {path}")

    batch = []
    batch_size = 10_000  # каждые 10k строк сбрасываем в файл
    total = 0

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            obj = safe_parse_json(raw_line)
            if obj is None:
                continue

            batch.append(obj)
            total += 1

            if len(batch) >= batch_size:
                table = pa.Table.from_pylist(batch)
                writer.write_table(table)
                batch.clear()

    # последний батч
    if batch:
        table = pa.Table.from_pylist(batch)
        writer.write_table(table)

    logger.info(f" → JSON записей добавлено: {total}")


# ============================================================
# Основная функция
# ============================================================

def load_all_audit_logs(archive_path: str):
    """
    Обрабатывает все архивы, все audit/, все .log,
    и сразу пишет всё в Parquet файл.
    """
    archive_path = Path(archive_path)

    work_dir = Path("temp_extract")
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir()

    parsed_dir = work_dir / "parsed"
    parsed_dir.mkdir()

    parquet_file = parsed_dir / "records.parquet"

    logger.info(f"Рабочая директория: {work_dir}")

    # первичная распаковка
    if not extract_zip(archive_path, work_dir) and \
       not extract_tar(archive_path, work_dir):
        raise Exception("Не удалось распаковать главный архив")

    expand_archives(work_dir)

    # Поиск папок audit
    audits = list(work_dir.rglob("audit"))
    if not audits:
        raise Exception("Папка audit не найдена")
    for a in audits:
        logger.info(f"audit найден: {a}")

    # поиск лог файлов
    log_files = []
    for a in audits:
        for lf in a.rglob("*.log"):
            logger.info(f"Лог найден: {lf}")
            log_files.append(lf)

    if not log_files:
        raise Exception("Нет .log файлов")

    # создаём writer
    logger.info("Создаём Parquet writer")
    writer = pq.ParquetWriter(
        parquet_file,
        schema=pa.schema([]),  # schema определяется автоматически при первой записи
        use_dictionary=True
    )

    # потоковая обработка
    for lf in log_files:
        stream_log_to_parquet(lf, writer)

    writer.close()

    logger.info(f"Стриминговая выгрузка завершена. Файл: {parquet_file}")

    # теперь возвращаем уже parquet → df
    logger.info("Читаем Parquet в DataFrame")
    df = pd.read_parquet(parquet_file)

    return df
