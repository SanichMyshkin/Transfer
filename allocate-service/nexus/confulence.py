import os
from dotenv import load_dotenv



import requests
from bs4 import BeautifulSoup

load_dotenv()

CONF_URL = os.getenv('CONF_URL')
PAGE_ID = os.getenv("PAGE_ID")
USER = os.getenv("USER")
TOKEN = os.getenv("PASS")

url = f"{CONF_URL}/rest/api/content/{PAGE_ID}"
params = {"expand": "body.storage"}

r = requests.get(
    url,
    params=params,
    auth=(USER, TOKEN),
    timeout=30,
    verify=False,
)
r.raise_for_status()

html = r.json()["body"]["storage"]["value"]

soup = BeautifulSoup(html, "html.parser")

table = soup.find("table")
if not table:
    raise RuntimeError("Таблица не найдена")

data = []
for tr in table.find_all("tr"):
    row = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
    if row:
        data.append(row)

headers = data[0]
rows = data[1:]

result = [dict(zip(headers, row)) for row in rows]

print(result)
