import json
import requests

API = "http://127.0.0.1:8001/v1/query"

questions = [
    "How many hours did Akash Brar work in January 2025?",
    "Who worked on Tiny Boards LP on January 2nd 2025?",
    "List the top 5 clients by total time in March 2025.",
    "How many hours were logged for Client 'VSBLTY Groupe Technologies Corp.' in 2025?",
    "Who did bookkeeping work for Neotech Metals Corp. (formerly Caravan) in February 2025?",
    "How many hours did Payroll Specialist Danielle Mananzan log in January 2025?",
    "List distinct task types for Treewalk (Internal) in April 2025.",
    "Show me 5 most recent entries for Zefiro Methane Corp.",
    "Top 10 team members by hours in 2025.",
    "How many hours of 'Treewalk: Administration' happened in January 2025?",
]

for q in questions:
    print("\nQ:", q)
    r = requests.post(API, json={"text": q, "user": "Mo", "channel": "auto"}, timeout=120)
    print("status", r.status_code)
    if r.status_code >= 400:
        print(r.text[:1000])
        continue
    data = r.json()
    print("Reply:", data.get("reply"))
    print("Plan:", json.dumps(data.get("plan"), indent=2))
    print("Diagnostics:", json.dumps(data.get("diagnostics"), indent=2))
