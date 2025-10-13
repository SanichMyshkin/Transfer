import os
import logging
from logging.handlers import TimedRotatingFileHandler
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import ENABLED
from minio.lifecycleconfig import (
    LifecycleConfig,
    Rule,
    Expiration,
    NoncurrentVersionExpiration,
)
from dotenv import load_dotenv


"""
Cкрипт задает правила по принципу 
mc ilm add minio1/example --noncurrent-expire-days 1 --expire-delete-marker
"""

log_filename = os.path.join(
    os.path.dirname(__file__), "logs", "minio-delete-markers.log"
)
os.makedirs(os.path.dirname(log_filename), exist_ok=True)

file_handler = TimedRotatingFileHandler(
    log_filename, when="midnight", interval=1, backupCount=7, encoding="utf-8"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[file_handler, logging.StreamHandler()],
)


load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
PREFIX = os.getenv("PREFIX", None)
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"


if MINIO_ENDPOINT is None or ACCESS_KEY is None or SECRET_KEY is None:
    logging.error(
        f"💥 MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY must be set {100 * '='}\n"
    )
    exit(1)


client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False,
)


def check_and_fix_lifecycle(bucket_name: str):
    """Добавляет правило для delete markers и/или noncurrent версий, если их нет."""
    if PREFIX and not bucket_name.startswith(PREFIX):
        logging.info(
            f"⏭ Пропускаем бакет {bucket_name} — не соответствует префиксу {PREFIX}"
        )
        return
    logging.info(f"🔍 Обрабатываем бакет {bucket_name}")

    try:
        policy = client.get_bucket_lifecycle(bucket_name)
    except S3Error as e:
        if e.code == "NoSuchLifecycleConfiguration":
            policy = None
        else:
            logging.error(f"❌ Ошибка при получении lifecycle для {bucket_name}: {e}")
            return

    # корректно обрабатываем разные форматы
    if not policy:
        lifecycle = LifecycleConfig([])
    elif isinstance(policy, LifecycleConfig):
        lifecycle = policy
    else:
        lifecycle = LifecycleConfig.fromxml(policy.decode("utf-8"))

    # проверяем, есть ли уже правила
    has_delete_marker_rule = any(
        getattr(rule.expiration, "expired_object_delete_marker", False)
        for rule in lifecycle.rules
        if rule.status == ENABLED
    )
    has_noncurrent_rule = any(
        getattr(rule.noncurrent_version_expiration, "noncurrent_days", None) == 1
        for rule in lifecycle.rules
        if rule.status == ENABLED
    )

    if has_delete_marker_rule and has_noncurrent_rule:
        logging.info(f"ℹ️ {bucket_name}: нужные правила уже существуют")
        return

    logging.warning(f"⚠️ {bucket_name}: lifecycle правило отсутствует или неполное")

    if DRY_RUN:
        logging.info(f"🧪 [DRY RUN] Добавил бы lifecycle правило для {bucket_name}")
        return

    logging.info(f"🔧 Добавляю missing правила для {bucket_name}")

    # создаём rule только с отсутствующими параметрами
    expiration = (
        Expiration(days=0, expired_object_delete_marker=True)
        if not has_delete_marker_rule
        else None
    )
    noncurrent_exp = (
        NoncurrentVersionExpiration(noncurrent_days=1)
        if not has_noncurrent_rule
        else None
    )

    rule = Rule(
        status=ENABLED,
        expiration=expiration,
        noncurrent_version_expiration=noncurrent_exp,
    )

    lifecycle.rules.append(rule)
    client.set_bucket_lifecycle(bucket_name, lifecycle)
    logging.info(f"✅ Lifecycle правило обновлено в {bucket_name}")


def main():
    buckets = [b.name for b in client.list_buckets()]
    logging.info(f"Найдено бакетов: {len(buckets)}")
    logging.info(
        f"Режим: {'🧪 DRY-RUN (только проверка)' if DRY_RUN else 'LIVE (вносятся изменения)'}"
    )

    for b in buckets:
        check_and_fix_lifecycle(b)


if __name__ == "__main__":
    main()
    logging.info(f"🧾 Обработка завершена {100 * '='}\n")
