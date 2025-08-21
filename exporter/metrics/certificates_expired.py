from datetime import datetime, timezone
from collections import Counter
from common.logs import logging
from prometheus_client import Gauge
from metrics.utils.api import get_from_nexus


CERT_DAYS_LEFT = Gauge(
    "nexus_cert_days_left",
    "Сколько дней осталось до истечения SSL-сертификатов в Nexus truststore",
    [
        "subject_common_name",
        "issuer_common_name",
        "fingerprint",
        "pem_short",
        "duplicates",
    ],
)


def clean_pem(pem: str) -> str:
    """Возвращает чистый base64 сертификата без BEGIN/END и пробелов"""
    if not pem:
        return ""
    return (
        pem.replace("-----BEGIN CERTIFICATE-----", "")
        .replace("-----END CERTIFICATE-----", "")
        .replace("\n", "")
        .replace(" ", "")
    )


def short_pem(pem: str) -> str:
    """Обрезает сертификат PEM для отображения"""
    pem_clean = clean_pem(pem)
    if not pem_clean:
        return "(none)"
    if len(pem_clean) <= 8:
        return pem_clean
    return f"{pem_clean[:6]}...{pem_clean[-6:]}"


def fetch_cert_lifetime_metrics(nexus_url: str, auth: tuple):
    # 💡 очищаем метрику перед пушем новых значений
    CERT_DAYS_LEFT.clear()

    try:
        certs = get_from_nexus(nexus_url, "security/ssl/truststore", auth)
    except Exception as e:
        logging.error(f"Ошибка при получении truststore из Nexus: {e}")
        return

    if not certs:
        logging.warning("⚠️ Truststore пустой — метрики по сертификатам не обновлены")
        return

    now = datetime.now(timezone.utc)

    # Строим ключ для поиска дублей (fingerprint + CN + Issuer + cert body)
    duplicate_keys = [
        (
            c.get("fingerprint", "(none)"),
            c.get("subjectCommonName", "(unknown)"),
            c.get("issuerCommonName", "(unknown)"),
            clean_pem(c.get("pem", "")),
        )
        for c in certs
    ]
    duplicate_counts = Counter(duplicate_keys)

    for cert in certs:
        try:
            expires_on_ms = cert.get("expiresOn")
            if not expires_on_ms:
                logging.warning(
                    f"❌ У сертификата {cert.get('fingerprint')} нет поля expiresOn"
                )
                continue

            # Конвертим время из мс
            expires_dt = datetime.fromtimestamp(expires_on_ms / 1000, tz=timezone.utc)
            days_left = (expires_dt - now).days

            subject_cn = cert.get("subjectCommonName", "(unknown)")
            issuer_cn = cert.get("issuerCommonName", "(unknown)")
            fingerprint = cert.get("fingerprint", "(none)")
            pem_clean = clean_pem(cert.get("pem", ""))
            pem_short = short_pem(cert.get("pem", ""))

            # Ключ для дублей
            key = (fingerprint, subject_cn, issuer_cn, pem_clean)
            duplicates = max(duplicate_counts[key] - 1, 0)

            # Пишем метрику
            CERT_DAYS_LEFT.labels(
                subject_common_name=subject_cn,
                issuer_common_name=issuer_cn,
                fingerprint=fingerprint,
                pem_short=pem_short,
                duplicates=str(duplicates),
            ).set(days_left)

            logging.info(
                f"📜 Сертификат CN='{subject_cn}' (Issuer='{issuer_cn}') "
                f"Fingerprint={fingerprint} PEM={pem_short} "
                f"истекает {expires_dt}, осталось {days_left} дней, дублей={duplicates}"
            )
        except Exception as e:
            logging.error(f"Ошибка обработки сертификата {cert}: {e}")
