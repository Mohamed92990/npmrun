from dotenv import dotenv_values
import requests

cfg = dotenv_values(r"C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project\.env")
token = (cfg.get("AIRTABLE_TOKEN") or "").strip()
base = (cfg.get("AIRTABLE_BASE_ID") or "").strip()
table = (cfg.get("AIRTABLE_TABLE_ID") or "").strip()
view = (cfg.get("AIRTABLE_VIEW_ID") or "").strip() or None

if not (token and base and table):
    raise SystemExit("Missing Airtable config in .env")

headers = {"Authorization": f"Bearer {token}"}
url = f"https://api.airtable.com/v0/{base}/{table}"

formula = "SEARCH('Bayridge', {Client})"
params = {"pageSize": 50, "filterByFormula": formula}
if view:
    params["view"] = view

r = requests.get(url, headers=headers, params=params, timeout=30)
print("status", r.status_code)
if r.status_code >= 400:
    print(r.text[:500])
    raise SystemExit(1)

data = r.json()
recs = data.get("records", [])
print("records", len(recs))

for rec in recs[:10]:
    f = rec.get("fields", {})
    out = {k: f.get(k) for k in ["Client", "Date", "Role", "Task_Type", "Team_Member", "Work", "Time_Minutes", "Cost"] if k in f}
    print(out)
