import os
import requests
import tempfile
import logging
import random
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

NEXUS_URL = os.getenv("NEXUS_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
RAW_REPO = os.getenv("RAW_REPO", "source-raw")

FILE_EXTENSIONS = [".txt", ".log", ".yaml", ".json", ".html", ".cfg", ".md", ".xml"]

def generate_random_path(index):
    """Генерирует путь с разной вложенностью."""
    depth = random.randint(0, 5)  # от корня до 5 уровней
    parts = [f"dir{random.randint(1, 5)}" for _ in range(depth)]
    filename = f"file{index}{random.choice(FILE_EXTENSIONS)}"
    return Path(*parts) / filename

def generate_content(size):
    """Генерирует строку указанного размера."""
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789\n ', k=size))

def upload_raw_file(file_path, relative_path):
    upload_url = f"{NEXUS_URL}/repository/{RAW_REPO}/{relative_path.as_posix()}"
    log.info(f"⬆️ Загружаем {relative_path}")

    with open(file_path, "rb") as f:
        resp = requests.put(
            upload_url,
            auth=(USERNAME, PASSWORD),
            headers={"Content-Type": "application/octet-stream"},
            data=f,
            verify=False,
        )

    if resp.status_code not in (200, 201, 204):
        log.error(f"❌ Ошибка загрузки {relative_path}: {resp.status_code} {resp.text}")
    else:
        log.info("✅ Успешно загружен")

def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir)

        for i in range(50):
            relative_path = generate_random_path(i)
            full_path = base_path / relative_path
            full_path.parent.mkdir(parents=True, exist_ok=True)

            file_size = random.randint(100, 50000)  # от 100 байт до 50 КБ
            full_path.write_text(generate_content(file_size))

            upload_raw_file(full_path, relative_path)

if __name__ == "__main__":
    main()
