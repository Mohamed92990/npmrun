import json
import requests

API = "http://127.0.0.1:8001/v1/query"

questions = [
    "Who worked on Tiny Boards LP on January 2nd 2025?",
    "How many hours did Akash Brar work in January 2025 (all clients)?",
    "Total cost for Atomic Development Inc. in January 2025",
    "List the top 5 clients by total time (hours) in January 2025",
    "Who did bookkeeping work for Bayridge Resources Corp. in January 2025?",
]

for q in questions:
    print("\nQ:", q)
    r = requests.post(API, json={"text": q, "user": "Mo", "channel": "regression"}, timeout=60)
    print("status", r.status_code)
    if r.status_code >= 400:
        print(r.text[:500])
        continue
    data = r.json()
    print("Reply:", data.get("reply"))
    print("Plan:", json.dumps(data.get("plan"), indent=2))
    print("Diagnostics:", json.dumps(data.get("diagnostics"), indent=2))
