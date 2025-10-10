import os
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

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
ACCESS_KEY = os.getenv("ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("SECRET_KEY", "minioadmin")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False,
)


def check_and_fix_lifecycle(bucket_name: str):
    """Добавляет правило: удалить delete markers и noncurrent версии через 1 день."""
    try:
        policy = client.get_bucket_lifecycle(bucket_name)
    except S3Error as e:
        if e.code == "NoSuchLifecycleConfiguration":
            policy = None
        else:
            print(f"[!] Ошибка при получении lifecycle для {bucket_name}: {e}")
            return

    # корректно обрабатываем разные форматы возврата
    if not policy:
        lifecycle = LifecycleConfig([])
    elif isinstance(policy, LifecycleConfig):
        lifecycle = policy
    else:
        lifecycle = LifecycleConfig.fromxml(policy.decode("utf-8"))

    found = any(
        (
            getattr(rule.expiration, "expired_object_delete_marker", False)
            and getattr(rule.noncurrent_version_expiration, "noncurrent_days", None)
            == 1
        )
        for rule in lifecycle.rules
        if rule.status == ENABLED
    )

    if found:
        print(f"[OK] {bucket_name}: нужное правило уже существует")
        return

    print(f"[WARN] {bucket_name}: lifecycle правило отсутствует")

    if DRY_RUN:
        print(f"[DRY-RUN] Добавил бы lifecycle правило для {bucket_name}")
        return

    print(f"[*] Добавляю lifecycle правило для {bucket_name}")

    expiration = Expiration(days=0, expired_object_delete_marker=True)
    noncurrent_exp = NoncurrentVersionExpiration(noncurrent_days=1)

    rule = Rule(
        # rule_id="auto-delete-markers-and-old-versions",
        status=ENABLED,
        expiration=expiration,
        noncurrent_version_expiration=noncurrent_exp,
    )

    lifecycle.rules.append(rule)
    client.set_bucket_lifecycle(bucket_name, lifecycle)
    print(f"[SUCCESS] Lifecycle правило добавлено в {bucket_name}")


def main():
    buckets = [b.name for b in client.list_buckets()]
    print(f"Найдено бакетов: {len(buckets)}")
    print(
        f"Режим: {'DRY-RUN (только проверка)' if DRY_RUN else 'LIVE (вносятся изменения)'}"
    )

    for b in buckets:
        check_and_fix_lifecycle(b)


if __name__ == "__main__":
    main()
