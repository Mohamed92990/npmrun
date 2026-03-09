import json

import requests

from app.core.config import OPENAI_API_KEY, OPENAI_MODEL
from app.models.query_plan import QueryPlan


SYSTEM = (
    "You translate natural-language questions about a timesheets database into a strict JSON QueryPlan. "
    "Return ONLY valid JSON, no markdown, no extra text. "
    "You do NOT do math; you only describe what to query. "
    "Extract every relevant filter (person, client, work, role, task_type, fee_type) and date range (from_ymd, to_ymd) when present. "
    "Choose the correct op and group_by so the backend can answer any question: totals, breakdowns, top N, distinct values, or raw list."
)

SCHEMA_HINT = {
    "op": "sum | distinct | group_sum | top | list",
    "metric": "time_minutes | cost (required for sum, group_sum, top)",
    "group_by": "Team_Member | Client | Work | Role | Task_Type | Fee_Type | Month (required for group_sum, top, distinct)",
    "fields": "array of field names for list (optional)",
    "limit": "integer 1-200 (default 10 for list/top/group_sum, use 1 for 'the highest/single')",
    "person": "team member name (substring match)",
    "client": "client/company name (exact match)",
    "work": "work/project name",
    "role": "role name (e.g. Bookkeeper)",
    "task_type": "task type (e.g. Bookkeeping)",
    "fee_type": "Billable | Non-Billable (filter by type of hours)",
    "from_ymd": "YYYY-MM-DD inclusive start",
    "to_ymd": "YYYY-MM-DD exclusive end",
}

RULES = """
- op choice:
  - Total/sum of hours (one number): op='sum'. Use for "how many hours", "total time", "billable hours for X", "non-billable hours for Y".
  - Breakdown/split by dimension: op='group_sum' with group_by set (e.g. Client, Work, Month, Team_Member). Use for "breakdown by client", "hours per project", "by month", "by person".
  - Top N / highest / most: op='top' with group_by and limit (e.g. "client with highest hours" -> group_by='Client', limit=1).
  - Who / which distinct values: op='distinct' with group_by (e.g. "who worked" -> group_by='Team_Member').
  - Raw rows/entries: op='list' when user wants to see individual records (with filters and limit).
- Always set metric='time_minutes' for hours-related questions (sum, group_sum, top).
- Date range: For "in January 2025", "Q1 2025", "this time period" (if year/month stated), set from_ymd and to_ymd (end exclusive). If only month name, backend can infer year.
- person: Set when user names a team member (e.g. "John's hours", "breakdown for Jane").
- client: Set to exact client/company name when mentioned (e.g. "for Acme Corp", "Atomic Development Inc.").
- fee_type: Set to "Billable" or "Non-Billable" when user asks for billable only or non-billable only.
- group_by for breakdowns: "by client" -> Client, "by project/work" -> Work, "by month" -> Month, "by person" -> Team_Member, "by role" -> Role, "by task type" -> Task_Type, "by fee type" -> Fee_Type.
- "Who was the bookkeeper for X" / "bookkeeping work": set task_type='Bookkeeping', not work.
- For "the client with the most hours" or "top 1 client": op='top', group_by='Client', limit=1.
- For "breakdown of hours by [person] in [period]": op='group_sum', group_by='Client' or 'Work' (or Month), person=that person, from_ymd/to_ymd.
"""

EXAMPLES = [
    ('Who was the bookkeeper for Atomic Development Inc. in January 2025?',
     '{"op":"distinct","group_by":"Team_Member","client":"Atomic Development Inc.","task_type":"Bookkeeping","from_ymd":"2025-01-01","to_ymd":"2025-02-01","limit":10}'),
    ('Top 5 clients by hours in March 2025',
     '{"op":"top","metric":"time_minutes","group_by":"Client","from_ymd":"2025-03-01","to_ymd":"2025-04-01","limit":5}'),
    ('Who was the client with the highest number of hours in January 2025?',
     '{"op":"top","metric":"time_minutes","group_by":"Client","from_ymd":"2025-01-01","to_ymd":"2025-02-01","limit":1}'),
    ('Give me a breakdown of hours worked by John in January 2025 by client',
     '{"op":"group_sum","metric":"time_minutes","group_by":"Client","person":"John","from_ymd":"2025-01-01","to_ymd":"2025-02-01","limit":50}'),
    ('Breakdown of hours by month for Acme Corp in 2025',
     '{"op":"group_sum","metric":"time_minutes","group_by":"Month","client":"Acme Corp","from_ymd":"2025-01-01","to_ymd":"2026-01-01","limit":12}'),
    ('How many non-billable hours did Jane work in March 2025?',
     '{"op":"sum","metric":"time_minutes","person":"Jane","fee_type":"Non-Billable","from_ymd":"2025-03-01","to_ymd":"2025-04-01"}'),
    ('How many billable hours did someone work for Atomic Development in Q1 2025?',
     '{"op":"sum","metric":"time_minutes","client":"Atomic Development","fee_type":"Billable","from_ymd":"2025-01-01","to_ymd":"2025-04-01"}'),
    ('Who had the most non-billable hours in January 2025?',
     '{"op":"top","metric":"time_minutes","group_by":"Team_Member","fee_type":"Non-Billable","from_ymd":"2025-01-01","to_ymd":"2025-02-01","limit":1}'),
    ('Hours per project for Sarah at Bayridge in February 2025',
     '{"op":"group_sum","metric":"time_minutes","group_by":"Work","person":"Sarah","client":"Bayridge","from_ymd":"2025-02-01","to_ymd":"2025-03-01","limit":50}'),
    ('Show me the top 10 team members by hours in 2025',
     '{"op":"top","metric":"time_minutes","group_by":"Team_Member","from_ymd":"2025-01-01","to_ymd":"2026-01-01","limit":10}'),
    ('List all bookkeeping entries for Client X in March 2025',
     '{"op":"list","client":"Client X","task_type":"Bookkeeping","from_ymd":"2025-03-01","to_ymd":"2025-04-01","limit":100}'),
    ('How many hours did the company work in total in April 2025?',
     '{"op":"sum","metric":"time_minutes","from_ymd":"2025-04-01","to_ymd":"2025-05-01"}'),
    ('Breakdown of billable vs non-billable hours for John',
     '{"op":"group_sum","metric":"time_minutes","group_by":"Fee_Type","person":"John","limit":10}'),
]


def parse_nl_to_plan(text: str) -> QueryPlan:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not set in .env")

    examples_block = "\n".join(
        f"Q: {q}\nA: {a}" for q, a in EXAMPLES
    )

    user = (
        f"Text: {text}\n\n"
        f"Output JSON matching this schema:\n{json.dumps(SCHEMA_HINT)}\n\n"
        f"Rules:\n{RULES}\n\n"
        f"Examples:\n{examples_block}"
    )

    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }

    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    content = r.json()["choices"][0]["message"]["content"]
    obj = json.loads(content)

    return QueryPlan.model_validate(obj)
