import time
import requests

BASE = "http://127.0.0.1:8041"

qs = [
    "Did Ubaid book PTO time in February 2026?",
    "What days and for how much time did Jay attend huddles in January 2026?",
    "What bookkeeping did Ubaid do in January 2026?",
]

for _ in range(50):
    try:
        if requests.get(f"{BASE}/healthz", timeout=2).status_code == 200:
            break
    except Exception:
        time.sleep(0.2)

for q in qs:
    r = requests.post(f"{BASE}/v1/query", json={"text": q}, timeout=90)
    print("\nQ:", q)
    print(r.status_code)
    print(r.json().get("reply"))
