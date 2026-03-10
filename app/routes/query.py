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
    t = (payload.text or "").lower()

    # If user asks who/bookkeeper/bookkeeping, force a safe shape.
    if ("bookkeeper" in t or "bookkeeping" in t) and not plan.task_type:
        plan.task_type = "Bookkeeping"

    # If user clearly says billable/non-billable and plan has no fee_type, set it.
    if not plan.fee_type:
        if "non-billable" in t or "nonbillable" in t or "non billable" in t:
            plan.fee_type = "Non-Billable"
        elif "billable" in t and "non-billable" not in t and "nonbillable" not in t:
            plan.fee_type = "Billable"

    # If a month name is present but the plan lacks a date range, infer the range.
    if not (plan.from_ymd and plan.to_ymd):
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
        if month_num:
            import re
            from datetime import datetime

            m = re.search(r"\b(20\d{2})\b", t)

            def _month_bounds(year: int):
                start = datetime(year, month_num, 1)
                end = datetime(year + 1, 1, 1) if month_num == 12 else datetime(year, month_num + 1, 1)
                return start.date().isoformat(), end.date().isoformat()

            # If year not provided, choose the most recent year available (prefer 2026, else 2025).
            if m:
                year = int(m.group(1))
            else:
                year = 2025
                try:
                    # Check if there is any data for this month in 2026 (under current filters).
                    from app.services.postgres_client import PostgresClient, load_pg_conn_info

                    VIEW = "public.karbon_timesheets_typed"
                    conninfo = load_pg_conn_info()
                    pg = PostgresClient(conninfo)

                    f26, t26 = _month_bounds(2026)

                    # Build filter where from the current plan
                    where = []
                    params = []
                    if plan.person:
                        where.append("team_member ILIKE %s")
                        params.append(f"%{plan.person}%")
                    if plan.client:
                        where.append("client = %s")
                        params.append(plan.client)
                    if plan.work:
                        where.append("work = %s")
                        params.append(plan.work)
                    if plan.role:
                        where.append("role ILIKE %s")
                        params.append(f"%{plan.role}%")
                    if plan.task_type:
                        where.append("task_type ILIKE %s")
                        params.append(f"%{plan.task_type}%")
                    if plan.fee_type:
                        where.append("fee_type ILIKE %s")
                        params.append(f"%{plan.fee_type}%")

                    where.append("date_ts >= %s::timestamptz AND date_ts < %s::timestamptz")
                    params.extend([f26, t26])

                    where_sql = " WHERE " + " AND ".join(where) if where else ""

                    with pg.connect() as conn, conn.cursor() as cur:
                        cur.execute(f"SELECT 1 FROM {VIEW}{where_sql} LIMIT 1", params)
                        if cur.fetchone():
                            year = 2026
                except Exception:
                    year = 2025

            f, t_ = _month_bounds(year)
            plan.from_ymd = f
            plan.to_ymd = t_

    # Rewrite common "what X" questions into safe distinct queries (better UX than listing rows).
    if plan.op == "list":
        if "what tasks" in t or "what task" in t:
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
