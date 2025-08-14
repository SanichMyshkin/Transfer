from common.logs import logging
from prometheus_client import Gauge
from metrics.utils.api import get_from_nexus
from database.cleanup_query import fetch_cleanup_name


# Метрика: 1 — политика используется, 0 — не используется
nexus_cleanup_policy_usage_gauge = Gauge(
    "nexus_cleanup_policy_used",
    "Nexus cleanup policy usage status (1 = used, 0 = unused)",
    ["policy_name"]
)


def fetch_cleanup_policy_usage(api_url: str, auth: tuple):
    """
    Собирает список всех политик очистки в Nexus, определяет, какие используются,
    и выставляет метрики Prometheus.
    
    :param api_url: URL API Nexus
    :param auth: авторизационные данные (tuple)
    """
    repository_settings = get_from_nexus(api_url, "repositorySettings", auth)
    used_policies = []

    # Сбор всех политик, реально используемых в репозиториях
    for repo in repository_settings or []:
        repo_name = repo.get("name") or repo.get("repositoryName") or "<unknown>"
        cleanup_data = repo.get("cleanup") or {}
        policy_names = cleanup_data.get("policyNames") or []

        if policy_names:
            logging.info("[✅] Репозиторий %s использует политики: %s", repo_name, ", ".join(policy_names))
            used_policies.extend(policy_names)
        else:
            logging.info("[➖] Репозиторий %s — политики очистки не заданы", repo_name)

    # Убираем дубликаты, сохраняем порядок
    unique_used_policies = list(dict.fromkeys(used_policies))
    logging.info(
        "Уникальные политики, которые используются: %s",
        ", ".join(unique_used_policies) if unique_used_policies else "—"
    )

    # Все политики из базы — приводим к строкам, если fetch_cleanup_name() возвращает кортежи
    all_policies = [p[0] if isinstance(p, tuple) else str(p) for p in fetch_cleanup_name()]
    logging.info("Все политики из базы: %s", ", ".join(all_policies) if all_policies else "—")

    # 🔹 Очищаем старые метрики перед установкой новых
    nexus_cleanup_policy_usage_gauge.clear()

    # Выставляем метрики и логируем смайликами
    for policy in all_policies:
        is_used = 1 if policy in unique_used_policies else 0
        nexus_cleanup_policy_usage_gauge.labels(policy_name=policy).set(is_used)

        # Выводим в лог смайлик вместо 0/1
        log_symbol = "✅" if is_used else "❌"
        logging.info("[📊] Политика '%s' -> %s", policy, log_symbol)

    return unique_used_policies
