from fastapi import APIRouter, Header, HTTPException

from app.core.config import WEBHOOK_SECRET
from app.models.query_plan import NLQueryIn, QueryOut
from app.services.nl_parser import parse_nl_to_plan
from app.services.query_engine_pg import execute_plan_pg

router = APIRouter()


@router.post("/query", response_model=QueryOut)
def query(payload: NLQueryIn, x_webhook_secret: str | None = Header(default=None)):
    if WEBHOOK_SECRET and x_webhook_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Missing/invalid X-Webhook-Secret")

    if not (payload.text or "").strip():
        raise HTTPException(status_code=400, detail="Missing 'text' in request body")

    plan = parse_nl_to_plan(payload.text)

    # Deterministic patch-ups for common phrases to improve reliability.
    # Normalize common Teams artifacts (HTML entities, non-breaking spaces, extra whitespace)
    raw_text = (payload.text or "")
    raw_text = raw_text.replace("&nbsp;", " ").replace("\u00a0", " ")
    raw_text = " ".join(raw_text.split())
    t = raw_text.lower()

    # If user asks who/bookkeeper/bookkeeping, force a safe shape.
    if ("bookkeeper" in t or "bookkeeping" in t) and not plan.task_type:
        plan.task_type = "Bookkeeping"

    # If user clearly says billable/non-billable and plan has no fee_type, set it.
    if not plan.fee_type:
        if "non-billable" in t or "nonbillable" in t or "non billable" in t:
            plan.fee_type = "Non-Billable"
        elif "billable" in t and "non-billable" not in t and "nonbillable" not in t:
            plan.fee_type = "Billable"

    # Day-specific handling: if the user mentions an explicit date (e.g. "Feb 2nd 2026"),
    # force a single-day range [date, date+1) so we don't accidentally return the whole month.
    import re
    from datetime import datetime, timedelta

    month_map = {
        "january": 1,
        "jan": 1,
        "february": 2,
        "feb": 2,
        "march": 3,
        "mar": 3,
        "april": 4,
        "apr": 4,
        "may": 5,
        "june": 6,
        "jun": 6,
        "july": 7,
        "jul": 7,
        "august": 8,
        "aug": 8,
        "september": 9,
        "sep": 9,
        "sept": 9,
        "october": 10,
        "oct": 10,
        "november": 11,
        "nov": 11,
        "december": 12,
        "dec": 12,
    }

    def _parse_explicit_date(text: str) -> str | None:
        # ISO date first
        m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        # Month name formats: "Feb 2nd 2026", "February 2, 2026", "March 3rd 2026"
        m = re.search(
            r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|"
            r"sep(?:tember)?|sept(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?\s+"
            r"(\d{1,2})(?:st|nd|rd|th)?(?:,)?\s+(20\d{2})\b",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            month = month_map[m.group(1).lower().replace(".", "")]
            day = int(m.group(2))
            year = int(m.group(3))
            try:
                return datetime(year, month, day).date().isoformat()
            except Exception:
                return None

        return None

    explicit_day = _parse_explicit_date(t)
    if explicit_day:
        d0 = datetime.fromisoformat(explicit_day)
        d1 = (d0 + timedelta(days=1)).date().isoformat()
        plan.from_ymd = explicit_day
        plan.to_ymd = d1

        # If user asked "what tasks did ... work on <date>", prefer a time breakdown by Work.
        if ("what task" in t or "what tasks" in t):
            plan.op = "group_sum"
            plan.group_by = "Work"
            plan.metric = plan.metric or "time_minutes"

        # If user asked for total time/hours on a specific day, force a sum.
        if ("how many" in t or "total" in t) and ("hour" in t or "hours" in t or "time" in t):
            plan.op = "sum"
            plan.metric = plan.metric or "time_minutes"

    # Month handling: if the user mentions a month but does not mention a year,
    # default to the most recent year available in the DB (prefer 2026, else 2025).
    # IMPORTANT: this should override any LLM-guessed year to keep behavior consistent.
    months = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    month_num = next((m for name, m in months.items() if name in t), None)
    if month_num and not explicit_day:
        import re
        from datetime import datetime

        year_match = re.search(r"\b(20\d{2})\b", t)

        def _month_bounds(year: int):
            start = datetime(year, month_num, 1)
            end = datetime(year + 1, 1, 1) if month_num == 12 else datetime(year, month_num + 1, 1)
            return start.date().isoformat(), end.date().isoformat()

        if year_match:
            # User explicitly gave a year; respect it.
            year = int(year_match.group(1))
            f, t_ = _month_bounds(year)
            plan.from_ymd, plan.to_ymd = f, t_
        else:
            # No year specified: choose most recent year present for that month.
            year = 2025
            try:
                from app.services.postgres_client import PostgresClient, load_pg_conn_info

                VIEW = "public.karbon_timesheets_typed"
                conninfo = load_pg_conn_info()
                pg = PostgresClient(conninfo)

                f26, t26 = _month_bounds(2026)
                with pg.connect() as conn, conn.cursor() as cur:
                    cur.execute(
                        f"SELECT 1 FROM {VIEW} WHERE date_ts >= %s::timestamptz AND date_ts < %s::timestamptz LIMIT 1",
                        [f26, t26],
                    )
                    if cur.fetchone():
                        year = 2026
            except Exception:
                year = 2025

            f, t_ = _month_bounds(year)
            plan.from_ymd, plan.to_ymd = f, t_

    # Rewrite common "what X" questions into safe distinct queries (better UX than listing rows).
    # If the user provided an explicit day, do NOT rewrite "what tasks" into distinct Task_Type.
    if plan.op == "list":
        if ("what tasks" in t or "what task" in t) and not explicit_day:
            plan.op = "distinct"
            plan.group_by = "Task_Type"
        elif "what clients" in t or "which clients" in t:
            plan.op = "distinct"
            plan.group_by = "Client"
        elif "what work" in t or "what projects" in t or "which projects" in t:
            plan.op = "distinct"
            plan.group_by = "Work"

    if "who" in t and plan.op == "list":
        # If they asked "who", listing rows is usually not what they want.
        plan.op = "distinct"
        plan.group_by = plan.group_by or "Team_Member"

    if plan.op == "distinct" and not plan.group_by:
        plan.group_by = "Team_Member"

    # group_sum/top/bottom need a metric; default to time for hours-style questions.
    if plan.op in ("group_sum", "top", "bottom") and not plan.metric:
        plan.metric = "time_minutes"

    # Deterministic fix: "least/lowest" should not use top.
    if ("least" in t or "lowest" in t or "minimum" in t) and plan.op == "top":
        plan.op = "bottom"
        plan.group_by = plan.group_by or "Team_Member"
        # If they asked for a single result, keep limit=1
        if "top" not in t and "5" not in t and "10" not in t and plan.limit:
            # leave the parser-provided limit; common case is limit=1
            pass

    # Guardrail: if the plan has no filters and is asking to list rows, don't dump random records.
    has_filter = any(
        [
            plan.person,
            plan.client,
            plan.work,
            plan.role,
            plan.task_type,
            plan.fee_type,
            plan.from_ymd,
            plan.to_ymd,
        ]
    )
    if plan.op == "list" and not has_filter:
        return QueryOut(
            reply="I didn’t catch the question details (client/person/date/fee type). Please re-send with more context, e.g. 'Who was the bookkeeper for Atomic Development Inc. in January 2025?' or 'List non-billable entries for John in March 2025.'",
            plan=plan,
            diagnostics={"reason": "empty_filters_prevent_list_dump"},
            data=None,
        )

    result = execute_plan_pg(plan, raw_text=payload.text)

    return QueryOut(
        reply=result.get("reply", ""),
        plan=plan,
        diagnostics=result.get("diagnostics", {}),
        data=result.get("data"),
    )
