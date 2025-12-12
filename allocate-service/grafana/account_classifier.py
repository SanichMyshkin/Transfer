import re
import pymorphy2

morph = pymorphy2.MorphAnalyzer()

LATIN_RE = re.compile(r"[A-Za-z]")
BANNED_WORDS_RE = re.compile(
    r"(служба|отдел|поддержк|администратор)",
    re.IGNORECASE,
)
CYRILLIC_RE = re.compile(r"^[А-Яа-яЁё\s-]+$")


def is_valid_domain(email: str, allowed_domains: list[str]) -> bool:
    if not email or "@" not in email:
        return False

    domain = email.split("@", 1)[1].lower()

    for allowed in allowed_domains:
        allowed = allowed.lower()
        if domain == allowed or domain.endswith("." + allowed):
            return True

    return False


def is_human_name(name: str) -> bool:
    if not name:
        return False

    name = name.strip()

    if LATIN_RE.search(name):
        return False

    if BANNED_WORDS_RE.search(name):
        return False

    if not CYRILLIC_RE.fullmatch(name):
        return False

    parts = name.replace("-", " ").split()
    if len(parts) not in (2, 3):
        return False

    valid_parts = 0

    for p in parts:
        parsed = morph.parse(p)[0]
        if (
            "Name" in parsed.tag
            or "Surn" in parsed.tag
            or "Patr" in parsed.tag
        ):
            valid_parts += 1

    return valid_parts >= 2


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
