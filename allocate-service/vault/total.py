import requests
import pandas as pd
from datetime import datetime

# --- настройки ---
VICTORIA_URL = "http://victoria.example.com:8428"
METRICS = [
    "vault_identity_entity_count",
    "vault_identity_entity_alias_count",
    "vault_secret_kv_count",
    "vault_token_count",
    "vault_token_count_by_auth",
]

# --- запрос одной метрики ---
def query_metric(metric):
    url = f"{VICTORIA_URL}/api/v1/query"
    resp = requests.get(url, params={"query": metric})
    data = resp.json()
    if data.get("status") != "success":
        print(f"[!] Ошибка при запросе {metric}: {data}")
        return []
    results = []
    for item in data["data"]["result"]:
        value = float(item["value"][1])
        labels = ", ".join([f"{k}={v}" for k, v in item["metric"].items() if k != "__name__"])
        results.append({"metric": metric, "labels": labels, "value": value})
    return results

# --- собираем все ---
all_results = []
for m in METRICS:
    all_results.extend(query_metric(m))

# --- формируем DataFrame ---
df = pd.DataFrame(all_results)
df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# --- сохраняем в Excel ---
output_file = f"vault_metrics_report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
df.to_excel(output_file, index=False)

print(f"[+] Отчёт сохранён: {output_file}")
