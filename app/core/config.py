import os
from pathlib import Path

from dotenv import dotenv_values

HERE = Path(__file__).resolve().parent.parent.parent
ENV_PATH = HERE / ".env"

CFG = {
    **dotenv_values(ENV_PATH),
    **os.environ,
}

AIRTABLE_TOKEN = (CFG.get("AIRTABLE_TOKEN") or "").strip()
AIRTABLE_BASE_ID = (CFG.get("AIRTABLE_BASE_ID") or "").strip()
AIRTABLE_TABLE_ID = (CFG.get("AIRTABLE_TABLE_ID") or "").strip()
AIRTABLE_VIEW_ID = (CFG.get("AIRTABLE_VIEW_ID") or "").strip() or None

OPENAI_API_KEY = (CFG.get("OPENAI_API_KEY") or "").strip() or None
OPENAI_MODEL = (CFG.get("OPENAI_MODEL") or "gpt-5.2").strip()

WEBHOOK_SECRET = (CFG.get("WEBHOOK_SECRET") or "").strip() or None

# allowlist of fields we will ever expose
ALLOWED_FIELDS = {
    "Client",
    "Work",
    "Team_Member",
    "Date",
    "Time_Minutes",
    "Cost",
    "Fee_Type",
    "Task_Type",
    "Role",
    "Notes",
    "Month",
    "WorkID",
}
