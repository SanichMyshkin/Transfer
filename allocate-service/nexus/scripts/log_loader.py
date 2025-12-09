import os
import shutil
import gzip
import tarfile
import zipfile
import sqlite3
import logging
from pathlib import Path
import json

logger = logging.getLogger("audit_loader")


def init_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            initiator TEXT,
            repo TEXT
        );
    """)

    conn.commit()
    return conn


def safe_json_parse(line: str):
    """Безопасный json.loads"""
    if "{" not in line or "}" not in line:
        return None
    if len(line) > 5_000_000:
        return None
    try:
        return json.loads(line)
    except Exception:
        return None


def extract_zip(path: Path, to: Path):
    try:
        with zipfile.ZipFile(path, "r") as z:
            z.extractall(to)
        return True
    except:
        return False


def extract_tar(path: Path, to: Path):
    try:
        with tarfile.open(path, "r:*") as t:
            t.extractall(to)
        return True
    except:
        return False


def extract_gzip(path: Path):
    out = path.with_suffix("")
    try:
        with gzip.open(path, "rb") as fin, open(out, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        return out
    except:
        return None


def expand_archives(root: Path):
    changed = True
    while changed:
        changed = False

        for file in list(root.rglob("*")):
            if not file.is_file():
                continue

            name = file.name.lower()

            if name.endswith(".zip"):
                logger.info(f"ZIP → {file}")
                if extract_zip(file, file.parent):
                    file.unlink()
                    changed = True
                continue

            if (
                name.endswith(".tar")
                or name.endswith(".tar.gz")
                or name.endswith(".tgz")
            ):
                logger.info(f"TAR → {file}")
                if extract_tar(file, file.parent):
                    file.unlink()
                    changed = True
                continue

            if name.endswith(".log.gz"):
                logger.info(f"GZIP LOG → {file}")
                out = extract_gzip(file)
                if out:
                    file.unlink()
                    changed = True
                continue


def load_log_file(path: Path, conn):
    logger.info(f"Чтение: {path}")

    cur = conn.cursor()
    batch = []
    count = 0

    # --- Диагностика формата initiator ---
    diagnostics = {
        "initiator_missing": 0,
        "initiator_string": 0,
        "initiator_dict": 0,
        "initiator_other": 0,
        "principal_found": 0,
        "createdBy_found": 0,
    }

    sample_print_limit = 10
    samples_printed = 0
    # --------------------------------------

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            jl = safe_json_parse(raw_line.strip())
            if not jl:
                continue

            # --- Диагностика initiator ---
            ini = jl.get("initiator")

            if ini is None:
                diagnostics["initiator_missing"] += 1
                # Печатаем примеры "пустых" инициаторов
                if samples_printed < sample_print_limit:
                    logger.warning("\n=== SAMPLE: initiator MISSING ===")
                    logger.warning(raw_line.strip())
                    samples_printed += 1

            elif isinstance(ini, str):
                diagnostics["initiator_string"] += 1

            elif isinstance(ini, dict):
                diagnostics["initiator_dict"] += 1

            else:
                diagnostics["initiator_other"] += 1

            if jl.get("authentication", {}).get("principal"):
                diagnostics["principal_found"] += 1

            if jl.get("createdBy"):
                diagnostics["createdBy_found"] += 1
            # ------------------------------------

            batch.append(
                (
                    jl.get("timestamp"),
                    jl.get("initiator"),  # <-- пока сохраняем как есть
                    jl.get("attributes", {}).get("repository.name")
                    or jl.get("attributes", {}).get("repositoryName"),
                )
            )

            count += 1

            if len(batch) >= 10_000:
                cur.executemany(
                    "INSERT INTO raw_logs(timestamp, initiator, repo) VALUES (?, ?, ?)",
                    batch,
                )
                conn.commit()
                batch.clear()

    if batch:
        cur.executemany(
            "INSERT INTO raw_logs(timestamp, initiator, repo) VALUES (?, ?, ?)", batch
        )
        conn.commit()

    logger.info(f" → JSON строк: {count}")

    # --- Вывод итоговой диагностики ---
    logger.warning("\n======== DIAGNOSTICS: initiator format analysis ========")
    for k, v in diagnostics.items():
        logger.warning(f"{k}: {v}")
    logger.warning("========================================================\n")


def load_all_audit_logs(archive_path: str):
    project_dir = Path(".")
    extract_dir = project_dir / "temp_extract"
    extract_dir.mkdir(exist_ok=True)

    db_path = project_dir / "temp_db" / "audit.db"
    db_path.parent.mkdir(exist_ok=True)

    logger.info(f"Создаём SQLite: {db_path}")
    conn = init_db(db_path)

    # первичное извлечение
    logger.info(f"Извлечение архива: {archive_path}")
    if not extract_zip(Path(archive_path), extract_dir) and not extract_tar(
        Path(archive_path), extract_dir
    ):
        raise Exception("Не удалось распаковать главный архив")

    expand_archives(extract_dir)

    audit_dirs = list(extract_dir.rglob("audit"))
    logger.info(f"Найдено audit директорий: {len(audit_dirs)}")

    for ad in audit_dirs:
        log_files = list(ad.rglob("*.log"))
        logger.info(f"В {ad} — {len(log_files)} логов")

        for lf in log_files:
            load_log_file(lf, conn)

    conn.commit()
    conn.close()

    logger.info("Готово. SQLite база заполнена.")
    return db_path
