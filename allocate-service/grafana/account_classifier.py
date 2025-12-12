import re
from natasha import NamesExtractor

name_extractor = NamesExtractor()

LATIN_RE = re.compile(r"[A-Za-z]")
BANNED_WORDS_RE = re.compile(r"(служба|отдел|поддержк|администратор)", re.I)
CYRILLIC_RE = re.compile(r"^[А-Яа-яЁё\s-]+$")


def is_valid_domain(email: str, allowed_domains: list[str]) -> bool:
    if "@" not in email:
        return False

    domain = email.split("@", 1)[1].lower()
    return any(domain == d or domain.endswith("." + d) for d in allowed_domains)


def is_human_name(name: str) -> bool:
    if not name:
        return False

    if LATIN_RE.search(name):
        return False

    if BANNED_WORDS_RE.search(name):
        return False

    if not CYRILLIC_RE.fullmatch(name.strip()):
        return False

    matches = list(name_extractor(name))
    if not matches:
        return False

    fact = matches[0].fact
    filled = sum(1 for p in (fact.first, fact.last, fact.middle) if p)

    return filled >= 2


def classify_unmatched_users(unmatched: list[dict], allowed_domains: list[str]):
    tech = []
    terminated = []

    for u in unmatched:
        email = (u.get("email") or "").strip().lower()
        name = (u.get("name") or "").strip()

        domain_ok = is_valid_domain(email, allowed_domains)
        human_name = is_human_name(name)

        if not domain_ok or not human_name:
            u["classification"] = "tech"
            tech.append(u)
        else:
            u["classification"] = "terminated"
            terminated.append(u)

    return tech, terminated
