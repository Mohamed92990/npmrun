# Karbon Timesheets Automation Project

Local-first backend for:
- Teams messages (NL questions) → deterministic DB answers (Airtable now; Postgres later)
- Karbon webhook ingestion → weekly hours/budget monitoring

## Setup

```powershell
cd "C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project"
python -m pip install -r requirements.txt
```

Copy the `.env` from the previous project (already copied here).

## Run

```powershell
cd "C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project"
python -m uvicorn app.main:app --host 127.0.0.1 --port 8001
```

Open docs:
- http://127.0.0.1:8001/docs

## Notes
- This project keeps the LLM as **parser only** (NL → QueryPlan JSON). All math/aggregation is deterministic.
