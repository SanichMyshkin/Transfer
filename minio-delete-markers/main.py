from minio import Minio
from minio.error import S3Error
from minio.commonconfig import ENABLED
from minio.lifecycleconfig import LifecycleConfig, Rule, Expiration


MINIO_ENDPOINT = "sanich.tech:8200"
ACCESS_KEY = "admin"
SECRET_KEY = "admin123"
AUTO_FIX = True


client = Minio(
    MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
)


def check_and_fix_lifecycle(bucket_name: str):
    """Проверяет lifecycle и при необходимости добавляет правило очистки delete markers."""
    try:
        policy = client.get_bucket_lifecycle(bucket_name)
    except S3Error as e:
        if e.code == "NoSuchLifecycleConfiguration":
            policy = None
        else:
            print(f"[!] Ошибка при получении lifecycle для {bucket_name}: {e}")
            return

    # Если lifecycle нет — создаём новый
    if not policy:
        print(f"[WARN] {bucket_name}: delete markers НЕ очищаются")
        if AUTO_FIX:
            print(f"[*] Добавляю lifecycle правило для {bucket_name}")
            expiration = Expiration(days=0, expired_object_delete_marker=True)
            rule = Rule(
                rule_id="auto-delete-markers", status=ENABLED, expiration=expiration
            )
            lifecycle = LifecycleConfig([rule])
            client.set_bucket_lifecycle(bucket_name, lifecycle)
            print(f"[+] Lifecycle правило добавлено в {bucket_name}")
        return

    # Если вернулся объект LifecycleConfig
    if isinstance(policy, LifecycleConfig):
        lifecycle = policy
    else:
        # Для старых версий SDK, которые возвращают XML
        lifecycle = LifecycleConfig.fromxml(policy.decode("utf-8"))

    # Проверяем, есть ли правило с delete markers
    found = False
    for rule in lifecycle.rules:
        exp = rule.expiration
        if exp and getattr(exp, "expired_object_delete_marker", False):
            found = True
            break

    if found:
        print(f"[OK] {bucket_name}: delete markers очищаются")
    else:
        print(f"[WARN] {bucket_name}: delete markers НЕ очищаются")
        if AUTO_FIX:
            print(f"[*] Добавляю lifecycle правило для {bucket_name}")
            expiration = Expiration(days=0, expired_object_delete_marker=True)
            rule = Rule(
                rule_id="auto-delete-markers", status=ENABLED, expiration=expiration
            )
            lifecycle.rules.append(rule)
            client.set_bucket_lifecycle(bucket_name, lifecycle)
            print(f"[+] Lifecycle правило добавлено в {bucket_name}")


def main():
    buckets = [b.name for b in client.list_buckets()]
    print(f"Найдено бакетов: {len(buckets)}")

    for b in buckets:
        check_and_fix_lifecycle(b)


if __name__ == "__main__":
    main()
