from __future__ import annotations

from datetime import datetime
import re

from app.models.query_plan import QueryPlan
from app.services.postgres_client import PostgresClient, load_pg_conn_info

# View created by supabase_create_view.sql
VIEW = "public.karbon_timesheets_typed"
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_SUPABASE = PROJECT_ROOT / ".env.supabase"


def month_range_ymd(month: int, year: int):
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)
    return start.date().strftime("%Y-%m-%d"), end.date().strftime("%Y-%m-%d")


def execute_plan_pg(plan: QueryPlan, raw_text: str = "") -> dict:
    conninfo = load_pg_conn_info(ENV_SUPABASE)
    pg = PostgresClient(conninfo)

    # Normalize empty strings to None (extra safety)
    person = (plan.person or "").strip() or None
    client = (plan.client or "").strip() or None

    # Normalize common client name misspellings to improve match reliability.
    def _normalize_client_name(s: str) -> str:
        t = s.strip()
        # common typo seen in queries
        t = re.sub(r"\bdevelopement\b", "Development", t, flags=re.IGNORECASE)
        return t

    if client:
        client = _normalize_client_name(client)
    work = (plan.work or "").strip() or None
    role = (plan.role or "").strip() or None
    task = (plan.task_type or "").strip() or None
    fee_type_filter = (plan.fee_type or "").strip() or None

    def _count(col: str, op: str, val: str, extra_where_sql: str, extra_params: list):
        # op is either '=' or 'ILIKE'
        with pg.connect() as conn, conn.cursor() as cur:
            if op == "=":
                cur.execute(f"SELECT COUNT(*) FROM {VIEW}{extra_where_sql} AND {col} = %s" if extra_where_sql else f"SELECT COUNT(*) FROM {VIEW} WHERE {col} = %s", extra_params + [val])
            else:
                cur.execute(f"SELECT COUNT(*) FROM {VIEW}{extra_where_sql} AND {col} ILIKE %s" if extra_where_sql else f"SELECT COUNT(*) FROM {VIEW} WHERE {col} ILIKE %s", extra_params + [f"%{val}%"])
            return cur.fetchone()[0]

    # Deterministic field-resolution: if the LLM puts an entity into the wrong bucket (work/client/task_type/role)
    # try to re-route it based on what actually exists in the DB.
    # We only do this when exactly one of these fields is populated.
    candidates = [("client", client), ("work", work), ("task", task), ("role", role)]
    populated = [(k, v) for k, v in candidates if v]

    from_ymd = (plan.from_ymd or "").strip() or None
    to_ymd = (plan.to_ymd or "").strip() or None

    where = []
    params = []

    # Build a "date range" where clause early for resolution counts
    range_where_sql = ""
    range_params: list = []
    if from_ymd and to_ymd:
        range_where_sql = " WHERE date_ts >= %s::timestamptz AND date_ts < %s::timestamptz"
        range_params = [from_ymd, to_ymd]

    if len(populated) == 1:
        k, v = populated[0]
        # Try exact matches first
        c_client = _count("client", "=", v, range_where_sql, range_params)
        c_work = _count("work", "=", v, range_where_sql, range_params)
        c_task = _count("task_type", "ILIKE", v, range_where_sql, range_params)
        c_role = _count("role", "ILIKE", v, range_where_sql, range_params)

        # Heuristic: if it looks like a task type (contains ':'), bias to task_type.
        looks_task = (":" in v)

        # Choose the best bucket deterministically
        scores = {
            "client": c_client,
            "work": c_work,
            "task": c_task + (10_000 if looks_task else 0),
            "role": c_role,
        }
        best = max(scores, key=lambda kk: scores[kk])
        if scores[best] > 0 and best != k:
            # re-route
            if best == "client":
                client, work, task, role = v, None, None, None
            elif best == "work":
                work, client, task, role = v, None, None, None
            elif best == "task":
                task, client, work, role = v, None, None, None
            elif best == "role":
                role, client, work, task = v, None, None, None

    if person:
        where.append("team_member ILIKE %s")
        params.append(f"%{person}%")
    if client:
        where.append("client = %s")
        params.append(client)
    if work:
        where.append("work = %s")
        params.append(work)
    if role:
        where.append("role ILIKE %s")
        params.append(f"%{role}%")
    if task:
        where.append("task_type ILIKE %s")
        params.append(f"%{task}%")
    if fee_type_filter:
        where.append("fee_type ILIKE %s")
        params.append(f"%{fee_type_filter}%")

    if from_ymd and to_ymd:
        where.append("date_ts >= %s::timestamptz AND date_ts < %s::timestamptz")
        params.extend([from_ymd, to_ymd])

    where_sql = " WHERE " + " AND ".join(where) if where else ""

    with pg.connect() as conn, conn.cursor() as cur:
        # matched count
        cur.execute(f"SELECT COUNT(*) FROM {VIEW}{where_sql}", params)
        matched = cur.fetchone()[0]

        if plan.op == "sum":
            if plan.metric == "cost":
                return {
                    "reply": "This dataset doesn’t include a cost field, so I can’t calculate total cost.",
                    "diagnostics": {"matched": matched},
                }
            # default time
            cur.execute(f"SELECT COALESCE(SUM(time_minutes),0) FROM {VIEW}{where_sql}", params)
            mins = int(round(float(cur.fetchone()[0] or 0)))
            h, m = mins // 60, mins % 60
            dur = f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")
            return {"reply": f"Total time is {dur}.", "diagnostics": {"matched": matched, "total_minutes": mins}}

        if plan.op == "distinct":
            gb = plan.group_by or "team_member"
            allowed = {"Team_Member": "team_member", "Client": "client", "Work": "work", "Role": "role", "Task_Type": "task_type", "Fee_Type": "fee_type"}
            col = allowed.get(gb, None)
            if not col:
                return {"reply": "I can only list distinct Team_Member/Client/Work/Role/Task_Type/Fee_Type.", "diagnostics": {"matched": matched}}
            cur.execute(f"SELECT DISTINCT {col} FROM {VIEW}{where_sql} AND {col} IS NOT NULL" if where_sql else f"SELECT DISTINCT {col} FROM {VIEW} WHERE {col} IS NOT NULL", params)
            vals = [r[0] for r in cur.fetchall()]
            vals = [v for v in vals if v]
            vals.sort()
            if not vals:
                return {"reply": "I couldn’t find anything matching that in the database.", "diagnostics": {"matched": matched}}
            # Human-friendly phrasing
            shown = vals[: plan.limit]
            if gb == "Team_Member":
                if len(shown) == 1:
                    reply = f"{shown[0]}"
                else:
                    reply = ", ".join(shown)
            else:
                reply = f"{gb} values: " + ", ".join(shown)

            return {"reply": reply, "diagnostics": {"matched": matched, "count": len(vals)}}

        if plan.op in ("group_sum", "top"):
            gb = plan.group_by or "Client"
            if plan.metric == "cost":
                return {"reply": "This dataset doesn’t include a cost field, so I can’t rank/group by cost.", "diagnostics": {"matched": matched}}
            metric_col = "time_minutes"

            # Month: group by year-month (date_trunc)
            if gb == "Month":
                extra = " AND date_ts IS NOT NULL" if where_sql else " WHERE date_ts IS NOT NULL"
                cur.execute(
                    f"SELECT date_trunc('month', date_ts)::date AS month_start, COALESCE(SUM({metric_col}),0) as v "
                    f"FROM {VIEW}{where_sql}{extra} "
                    f"GROUP BY date_trunc('month', date_ts) ORDER BY month_start ASC LIMIT %s",
                    params + [plan.limit],
                )
                items = cur.fetchall()
                if not items:
                    return {"reply": "I couldn’t find anything matching that in the database.", "diagnostics": {"matched": matched}}
                lines = []
                for row, v in items:
                    mins = int(round(float(v)))
                    h, m = mins // 60, mins % 60
                    dur = f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")
                    label = row.strftime("%Y-%m") if hasattr(row, "strftime") else str(row)
                    lines.append(f"- {label}: {dur}")
                title = f"Top {len(items)} months by time" if plan.op == "top" else "Time by month"
                return {"reply": title + ":\n" + "\n".join(lines), "diagnostics": {"matched": matched}}

            allowed = {"Team_Member": "team_member", "Client": "client", "Work": "work", "Role": "role", "Task_Type": "task_type", "Fee_Type": "fee_type"}
            col = allowed.get(gb, None)
            if not col:
                return {"reply": "I can only group by Team_Member/Client/Work/Role/Task_Type/Fee_Type/Month.", "diagnostics": {"matched": matched}}

            cur.execute(
                f"SELECT {col}, COALESCE(SUM({metric_col}),0) as v FROM {VIEW}{where_sql} GROUP BY {col} ORDER BY v DESC LIMIT %s",
                params + [plan.limit],
            )
            items = cur.fetchall()
            if not items:
                return {"reply": "I couldn’t find anything matching that in the database.", "diagnostics": {"matched": matched}}

            lines = []
            for k, v in items:
                mins = int(round(float(v)))
                h, m = mins // 60, mins % 60
                dur = f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")
                label = "(blank)" if k is None or (isinstance(k, str) and not k.strip()) else k
                lines.append(f"- {label}: {dur}")

            title = f"Top {len(items)} by time" if plan.op == "top" else f"Time by {gb}"
            return {"reply": title + ":\n" + "\n".join(lines), "diagnostics": {"matched": matched}}

        if plan.op == "list":
            cur.execute(
                f"SELECT date_ts, team_member, client, work, time_minutes, fee_type FROM {VIEW}{where_sql} ORDER BY date_ts DESC LIMIT %s",
                params + [plan.limit],
            )
            rows = cur.fetchall()
            data = [
                {
                    "Date": r[0].isoformat() if r[0] else None,
                    "Team_Member": r[1],
                    "Client": r[2],
                    "Work": r[3],
                    "Time_Minutes": float(r[4]) if r[4] is not None else None,
                    "Fee_Type": r[5] if len(r) > 5 else None,
                }
                for r in rows
            ]
            if not data:
                return {"reply": "I couldn’t find anything matching that in the database.", "diagnostics": {"matched": matched}, "data": []}
            return {"reply": f"Here are {len(data)} records.", "diagnostics": {"matched": matched}, "data": data}

        return {"reply": "Unsupported op.", "diagnostics": {"matched": matched}}
