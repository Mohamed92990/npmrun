import requests
q = "How much time was spent on Neotech Metals Corp. (formerly Caravan) in January and February 2026?"
r = requests.post("http://127.0.0.1:8040/v1/query", json={"text": q}, timeout=90)
print(r.status_code)
print(r.json().get("reply"))
print(r.json().get("plan"))
print(r.json().get("diagnostics"))
