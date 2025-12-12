import re

LATIN_RE = re.compile(r"[A-Za-z]")
BANNED_WORDS_RE = re.compile(
    r"(служба|отдел|поддержк|админ)",
    re.IGNORECASE,
)


def is_valid_domain(email: str, allowed_domains: list[str]) -> bool:
    if not email or "@" not in email:
        return False

    domain = email.split("@", 1)[1].lower()
    return any(domain == d or domain.endswith("." + d) for d in allowed_domains)


def is_human_name(name: str) -> bool:
    if not name:
        return False

    name = name.strip()

    if LATIN_RE.search(name):
        return False

    if BANNED_WORDS_RE.search(name):
        return False

    parts = re.split(r"\s+", name)

    if len(parts) not in (2, 3):
        return False

    for part in parts:
        if not re.fullmatch(r"[А-ЯЁ][а-яё\-]+", part):
            return False

    return True


def classify_unmatched_users(unmatched: list[dict], allowed_domains: list[str]):
    tech_accounts = []
    terminated_users = []

    for u in unmatched:
        email = (u.get("email") or "").strip().lower()
        name = (u.get("name") or "").strip()

        domain_ok = is_valid_domain(email, allowed_domains)
        human_name = is_human_name(name)

        if not domain_ok or not human_name:
            u["classification"] = "tech"
            tech_accounts.append(u)
        else:
            u["classification"] = "terminated"
            terminated_users.append(u)

    return tech_accounts, terminated_users
