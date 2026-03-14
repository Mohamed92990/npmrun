import time
import requests

BASE = "http://127.0.0.1:8042"
q = "What percentage of time in January 2026 was bookeeping?"

for _ in range(50):
    try:
        if requests.get(f"{BASE}/healthz", timeout=2).status_code == 200:
            break
    except Exception:
        time.sleep(0.2)

r = requests.post(f"{BASE}/v1/query", json={"text": q}, timeout=90)
print(r.status_code)
print(r.json().get("reply"))
print(r.json().get("plan"))
