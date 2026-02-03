import requests
from bs4 import BeautifulSoup


def confluence_table_as_dicts(conf_url, page_id, user, password):
    url = f"{conf_url.rstrip('/')}/rest/api/content/{page_id}"
    params = {"expand": "body.storage"}

    r = requests.get(
        url,
        params=params,
        auth=(user, password),
        headers={"User-Agent": "curl/7.88.1"},
        timeout=30,
        verify=False,
    )
    r.raise_for_status()

    html = r.json()["body"]["storage"]["value"]
    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table")
    if not table:
        raise RuntimeError("Таблица не найдена в Confluence")

    rows = []
    for tr in table.find_all("tr"):
        cols = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
        if cols:
            rows.append(cols)

    if len(rows) < 2:
        raise RuntimeError("Таблица пустая или без данных")

    headers = [c.strip() for c in rows[0]]
    out = []
    for row in rows[1:]:
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))
        out.append(dict(zip(headers, row[: len(headers)])))
    return out


def repo_to_service_map(conf_rows):
    def norm(s):
        return (s or "").strip().lower()

    repo_keys = {"repo", "repository", "repository_name", "репозиторий"}
    service_keys = {"service", "service_name", "сервис", "наименование сервиса"}

    m = {}
    for row in conf_rows:
        keys_norm = {norm(k): k for k in row.keys()}

        repo_col = None
        for k in repo_keys:
            if k in keys_norm:
                repo_col = keys_norm[k]
                break
        if repo_col is None:
            for k in keys_norm:
                if "repo" in k or "реп" in k:
                    repo_col = keys_norm[k]
                    break

        service_col = None
        for k in service_keys:
            if k in keys_norm:
                service_col = keys_norm[k]
                break
        if service_col is None:
            for k in keys_norm:
                if "service" in k or "сервис" in k:
                    service_col = keys_norm[k]
                    break

        repo_name = (row.get(repo_col) or "").strip() if repo_col else ""
        service_name = (row.get(service_col) or "").strip() if service_col else ""

        if repo_name:
            m[repo_name] = service_name

    return m
