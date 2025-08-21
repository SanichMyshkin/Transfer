## надо подумать как сразу серты закидывать а не мониторить
from common.logs import logging
from prometheus_client import Gauge
from metrics.utils.api import get_from_nexus


CERT_MATCH_STATUS = Gauge(
    "nexus_cert_url_match",
    "Совпадение SSL-сертификатов с remote URL в proxy-репозиториях Nexus",
    ["repo_name", "remote_url", "subject_common_name", "match_level"],
)


def match_level(cert_cn: str, remote_url: str) -> int:
    if not cert_cn or not remote_url:
        return 0
    base = cert_cn.strip("*.")  # wildcard
    if base in remote_url:
        return 1
    short = base.split(".")[0]
    if short in remote_url:
        return 2
    return 0


def update_cert_match_metrics(nexus_url: str, auth: tuple):
    CERT_MATCH_STATUS.clear()

    try:
        certs = get_from_nexus(nexus_url, "security/ssl/truststore", auth)
        repos = get_from_nexus(nexus_url, "repositories", auth)
    except Exception as e:
        logging.error(f"Ошибка при получении данных из Nexus: {e}")
        return

    if not certs or not repos:
        return

    repos = [
        {
            "name": r["name"],
            "remote": r.get("attributes", {}).get("proxy", {}).get("remoteUrl", ""),
        }
        for r in repos
        if r.get("type") == "proxy"
    ]

    for repo in repos:
        remote = repo["remote"]
        name = repo["name"]
        best_level = 0
        best_cert_cn = None

        for cert in certs:
            cn = cert.get("subjectCommonName", "")
            level = match_level(cn, remote)

            if level > best_level:
                best_level = level
                best_cert_cn = cn

        if best_level > 0 and best_cert_cn:
            CERT_MATCH_STATUS.labels(
                repo_name=name,
                remote_url=remote,
                subject_common_name=best_cert_cn,
                match_level=str(best_level),
            ).set(best_level)

            logging.info(
                f"✔️ Совпадение: Repo='{name}', URL='{remote}', CN='{best_cert_cn}', Уровень={best_level}"
            )
        else:
            CERT_MATCH_STATUS.labels(
                repo_name=name,
                remote_url=remote,
                subject_common_name="(none)",
                match_level="0",
            ).set(0)

            logging.info(
                f"⚠️ Нет совпадений: Repo='{name}', URL='{remote}' → ни один сертификат не подошёл"
            )