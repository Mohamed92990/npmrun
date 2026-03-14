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

    # Normalize common misspellings to improve match reliability.
    def _normalize_client_name(s: str) -> str:
        t = s.strip()
        # common typo seen in queries
        t = re.sub(r"\bdevelopement\b", "Development", t, flags=re.IGNORECASE)
        return t

    def _normalize_task_filter(s: str) -> str:
        t = s.strip().strip('"').strip("'")
        # Fix common spacing/typos
        t = t.replace("Bookkeeping:", "Bookkeeping: ")
        t = re.sub(r"\bcontinous\b", "Continuous", t, flags=re.IGNORECASE)
        t = re.sub(r"\bbookeeping\b", "Bookkeeping", t, flags=re.IGNORECASE)
        # collapse spaces
        t = " ".join(t.split())
        return t

    if client:
        client = _normalize_client_name(client)
    work = (plan.work or "").strip() or None
    role = (plan.role or "").strip() or None
    task = (plan.task_type or "").strip() or None
    if task:
        task = _normalize_task_filter(task)
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

    # Strict name handling: if only a first name is provided and it matches multiple staff,
    # refuse and ask for full name to avoid mixing people.
    if person and len(person.split()) == 1:
        with pg.connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT team_member FROM {VIEW}{range_where_sql}"
                + (" AND team_member ILIKE %s" if range_where_sql else " WHERE team_member ILIKE %s")
                + " ORDER BY team_member ASC LIMIT 25",
                range_params + [f"%{person}%"],
            )
            matches = [r[0] for r in cur.fetchall() if r and r[0] and str(r[0]).strip()]

        if len(matches) == 1:
            person = str(matches[0]).strip()
        elif len(matches) > 1:
            return {
                "reply": "Please query with full name as there are many staff members with this first name.",
                "diagnostics": {"person_matches": matches[:10], "count": len(matches)},
            }

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
        # Case-insensitive partial match (clients often queried by short name)
        where.append("client ILIKE %s")
        params.append(f"%{client}%")

    # Excluding Treewalk logic
    q_text = (raw_text or "").lower()
    wants_top_client = (
        ("which client" in q_text and "most" in q_text and ("hour" in q_text or "hours" in q_text))
        or ("client had the most" in q_text)
    )
    exclude_treewalk = (
        ("apart from treewalk" in q_text or "excluding treewalk" in q_text or "exclude treewalk" in q_text)
        or wants_top_client
    )
    if exclude_treewalk and not client:
        where.append("client NOT ILIKE %s")
        params.append("Treewalk%")
    if work:
        where.append("work = %s")
        params.append(work)
    if role:
        where.append("role ILIKE %s")
        params.append(f"%{role}%")

    # Build a denominator filter set for percent queries (everything except task_type).
    den_where = list(where)
    den_params = list(params)

    if task:
        tnorm = _normalize_task_filter(task)
        # Prefix-based category filter when caller uses a top-level category
        categories = {
            "Bookkeeping",
            "Consulting",
            "Financial Reporting",
            "Non-billable",
            "PTO",
            "Payments",
            "Payroll",
            "Tax",
            "Treewalk",
        }
        if tnorm in categories:
            where.append("task_type ILIKE %s")
            params.append(f"{tnorm}:%")
        else:
            where.append("task_type ILIKE %s")
            params.append(f"%{tnorm}%")
    if fee_type_filter:
        # Some exports encode non-billable work in Task Type labels (e.g. "Non-billable: File Transition")
        # even when Fee Type is missing/inconsistent. For Non-Billable queries, include both signals.
        ft = fee_type_filter.strip().lower()
        if "non" in ft and "bill" in ft:
            where.append("(fee_type ILIKE %s OR task_type ILIKE %s)")
            params.append("%Non-Billable%")
            params.append("Non-billable%")
            den_where.append("(fee_type ILIKE %s OR task_type ILIKE %s)")
            den_params.append("%Non-Billable%")
            den_params.append("Non-billable%")
        else:
            where.append("fee_type ILIKE %s")
            params.append(f"%{fee_type_filter}%")
            den_where.append("fee_type ILIKE %s")
            den_params.append(f"%{fee_type_filter}%")

    if from_ymd and to_ymd:
        where.append("date_ts >= %s::timestamptz AND date_ts < %s::timestamptz")
        params.extend([from_ymd, to_ymd])
        den_where.append("date_ts >= %s::timestamptz AND date_ts < %s::timestamptz")
        den_params.extend([from_ymd, to_ymd])

    where_sql = " WHERE " + " AND ".join(where) if where else ""

    with pg.connect() as conn, conn.cursor() as cur:
        # matched count
        cur.execute(f"SELECT COUNT(*) FROM {VIEW}{where_sql}", params)
        matched = cur.fetchone()[0]

        if plan.op == "percent":
            # numerator: with task filter; denominator: same filters without task filter
            if not task:
                return {"reply": "Tell me which task category you want a percentage for (e.g., bookkeeping, PTO, tax).", "diagnostics": {"matched": matched}}

            where_sql_num = " WHERE " + " AND ".join(where) if where else ""
            where_sql_den = " WHERE " + " AND ".join(den_where) if den_where else ""

            cur.execute(f"SELECT COALESCE(SUM(time_minutes),0) FROM {VIEW}{where_sql_num}", params)
            num = float(cur.fetchone()[0] or 0)
            cur.execute(f"SELECT COALESCE(SUM(time_minutes),0) FROM {VIEW}{where_sql_den}", den_params)
            den = float(cur.fetchone()[0] or 0)

            if den <= 0:
                return {"reply": "I couldn’t calculate a percentage because total time for that period was 0.", "diagnostics": {"matched": matched}}

            pct = (num / den) * 100.0

            def fmt_dur(mins_val: float) -> str:
                mins_i = int(round(float(mins_val)))
                h, m = mins_i // 60, mins_i % 60
                return f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")

            # Period label reuse from sum
            period_label = ""
            try:
                if from_ymd and to_ymd and len(from_ymd) >= 10:
                    d0 = datetime.fromisoformat(from_ymd).date()
                    d1 = datetime.fromisoformat(to_ymd).date()
                    month_names = {
                        "01": "January",
                        "02": "February",
                        "03": "March",
                        "04": "April",
                        "05": "May",
                        "06": "June",
                        "07": "July",
                        "08": "August",
                        "09": "September",
                        "10": "October",
                        "11": "November",
                        "12": "December",
                    }
                    if d0.day == 1 and d1.day == 1 and d0.year == d1.year:
                        sm = str(d0.month).zfill(2)
                        em = str((d1.month - 1) or 12).zfill(2)
                        if sm == em and sm in month_names:
                            period_label = f" in {month_names[sm]} {d0.year}"
            except Exception:
                period_label = ""

            cat = _normalize_task_filter(task)
            if cat in {"Bookkeeping","Consulting","Financial Reporting","Non-billable","PTO","Payments","Payroll","Tax","Treewalk"}:
                cat_label = cat.lower()
            else:
                cat_label = "that"

            reply = f"{cat_label.capitalize()} was {pct:.1f}% of total time{period_label} ({fmt_dur(num)} of {fmt_dur(den)})."
            return {"reply": reply, "diagnostics": {"matched": matched, "num_minutes": num, "den_minutes": den, "percent": pct}}

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
            # Natural reply
            period = ""
            try:
                if from_ymd and to_ymd and len(from_ymd) >= 10:
                    # If it looks like a single-day window [d, d+1), prefer "on <date>".
                    try:
                        d0 = datetime.fromisoformat(from_ymd).date()
                        d1 = datetime.fromisoformat(to_ymd).date()
                        day_span = (d1.toordinal() - d0.toordinal())
                        if day_span == 1:
                            period = f" on {d0.isoformat()}"
                        else:
                            # Whole-month windows like [2026-01-01, 2026-03-01)
                            month_names = {
                                "01": "January",
                                "02": "February",
                                "03": "March",
                                "04": "April",
                                "05": "May",
                                "06": "June",
                                "07": "July",
                                "08": "August",
                                "09": "September",
                                "10": "October",
                                "11": "November",
                                "12": "December",
                            }
                            if d0.day == 1 and d1.day == 1 and d0.year == d1.year:
                                sm = str(d0.month).zfill(2)
                                em = str((d1.month - 1) or 12).zfill(2)
                                if sm in month_names and em in month_names:
                                    if sm == em:
                                        period = f" in {month_names[sm]} {d0.year}"
                                    elif d1.month - d0.month == 2:
                                        period = f" in {month_names[sm]} and {month_names[em]} {d0.year}"
                                    else:
                                        period = f" from {month_names[sm]} to {month_names[em]} {d0.year}"
                            if not period:
                                # Fallback to start month label
                                y, mo, _ = from_ymd.split("-", 2)
                                mm = mo.zfill(2)
                                if mm in month_names:
                                    period = f" in {month_names[mm]} {y}"
                    except Exception:
                        # Fallback to month label
                        y, mo, _ = from_ymd.split("-", 2)
                        month_names = {
                            "01": "January",
                            "02": "February",
                            "03": "March",
                            "04": "April",
                            "05": "May",
                            "06": "June",
                            "07": "July",
                            "08": "August",
                            "09": "September",
                            "10": "October",
                            "11": "November",
                            "12": "December",
                        }
                        mm = mo.zfill(2)
                        if mm in month_names:
                            period = f" in {month_names[mm]} {y}"
            except Exception:
                period = ""

            subject_parts = []
            if person:
                subject_parts.append(person)
            if client:
                subject_parts.append(client)
            if work:
                subject_parts.append(work)

            if subject_parts:
                subj = " for ".join(subject_parts) if len(subject_parts) > 1 else subject_parts[0]
                q = (raw_text or "").lower()

                # Yes/no phrasing for questions like "Did X book PTO..." or "Did X attend huddles..."
                if (q.startswith("did ") or " did " in q) and ("pto" in q or "huddles" in q):
                    if mins <= 0 or matched == 0:
                        reply = f"No, {subj} did not log any {('PTO' if 'pto' in q else 'huddles')}{period}."
                    else:
                        reply = f"Yes, {subj} logged {dur} of {('PTO' if 'pto' in q else 'huddles')}{period}."

                # Special phrasing for non-billable sums
                elif fee_type_filter and ("non" in fee_type_filter.lower() and "bill" in fee_type_filter.lower()):
                    reply = f"{subj} had worked {dur} non billable hours{period}."

                # Better grammar when user asks "how many hours did we spend on <client>" / "was spent on"
                elif client and ("did we spend" in q or "we spend" in q or "spent on" in q or "was spent on" in q):
                    end = "" if str(client).strip().endswith(".") else "."
                    reply = f"We spent {dur} on {client}{period}{end}"

                else:
                    reply = f"{subj} worked {dur}{period}."
            else:
                reply = f"Total time{period} was {dur}."

            return {"reply": reply, "diagnostics": {"matched": matched, "total_minutes": mins}}

        if plan.op == "distinct":
            gb = plan.group_by or "team_member"
            allowed = {
                "Team_Member": "team_member",
                "Client": "client",
                "Work": "work",
                "Role": "role",
                "Task_Type": "task_type",
                "Fee_Type": "fee_type",
            }
            col = allowed.get(gb, None)
            if not col:
                return {
                    "reply": "I can list distinct people, clients, work items, roles, task types, or fee types.",
                    "diagnostics": {"matched": matched},
                }

            cur.execute(
                f"SELECT DISTINCT {col} FROM {VIEW}{where_sql} AND {col} IS NOT NULL"
                if where_sql
                else f"SELECT DISTINCT {col} FROM {VIEW} WHERE {col} IS NOT NULL",
                params,
            )
            vals = [r[0] for r in cur.fetchall()]
            vals = [v for v in vals if v and (not isinstance(v, str) or v.strip())]
            vals.sort()
            if not vals:
                return {"reply": "I couldn’t find anything matching that in the database.", "diagnostics": {"matched": matched}}

            # Limit + "and N more" behavior
            shown = vals[: plan.limit]
            extra_n = max(0, len(vals) - len(shown))

            def join_list(items: list[str]) -> str:
                if len(items) == 1:
                    return items[0]
                if len(items) == 2:
                    return f"{items[0]} and {items[1]}"
                return ", ".join(items[:-1]) + f", and {items[-1]}"

            # Friendly labels
            label_map = {
                "Team_Member": "People",
                "Client": "Clients",
                "Work": "Work items",
                "Role": "Roles",
                "Task_Type": "Task types",
                "Fee_Type": "Fee types",
            }
            label = label_map.get(gb, gb)

            # Slightly smarter phrasing based on the question
            q = (raw_text or "").lower()
            # Build a natural header + numbered list (no colons)
            header = None
            if gb == "Team_Member" and ("bookkeeper" in q or "bookkeeping" in q):
                header = "Bookkeepers"
            elif "what tasks" in q or "what task" in q:
                header = "Tasks"
            elif "what clients" in q or "which clients" in q:
                header = "Clients"
            elif "what work" in q or "what projects" in q or "which projects" in q:
                header = "Work items"
            elif "who" in q and gb == "Team_Member":
                header = "People"
            else:
                header = label

            # Add a little context if available
            context_bits = []
            if person:
                context_bits.append(f"for {person}")
            if client:
                context_bits.append(f"at {client}")
            if work:
                context_bits.append(f"on {work}")
            if from_ymd and to_ymd and len(from_ymd) >= 10:
                try:
                    y, mo, _ = from_ymd.split("-", 2)
                    month_names = {
                        "01": "January",
                        "02": "February",
                        "03": "March",
                        "04": "April",
                        "05": "May",
                        "06": "June",
                        "07": "July",
                        "08": "August",
                        "09": "September",
                        "10": "October",
                        "11": "November",
                        "12": "December",
                    }
                    mm = mo.zfill(2)
                    if mm in month_names:
                        context_bits.append(f"in {month_names[mm]} {y}")
                except Exception:
                    pass

            title = header
            if context_bits:
                title += " " + " ".join(context_bits)

            lines = [f"{i}) {v}" for i, v in enumerate(shown, start=1)]
            reply = title + "\n" + "\n".join(lines)

            if extra_n:
                reply += f"\nPlus {extra_n} more."

            return {"reply": reply, "diagnostics": {"matched": matched, "count": len(vals)}}

        if plan.op in ("group_sum", "top", "bottom"):
            gb = plan.group_by or "Client"
            if plan.metric == "cost":
                return {"reply": "This dataset doesn’t include a cost field, so I can’t rank/group by cost.", "diagnostics": {"matched": matched}}
            metric_col = "time_minutes"

            # Day: group by date
            if gb == "Date":
                extra = " AND date_ts IS NOT NULL" if where_sql else " WHERE date_ts IS NOT NULL"
                cur.execute(
                    f"SELECT date_ts::date AS day, COALESCE(SUM({metric_col}),0) as v "
                    f"FROM {VIEW}{where_sql}{extra} "
                    f"GROUP BY date_ts::date ORDER BY day ASC LIMIT %s",
                    params + [plan.limit],
                )
                items = cur.fetchall()
                if not items:
                    return {"reply": "I couldn’t find anything matching that in the database.", "diagnostics": {"matched": matched}}

                def fmt_dur(vmins: float) -> str:
                    mins = int(round(float(vmins)))
                    h, m = mins // 60, mins % 60
                    return f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")

                lines = []
                for i, (day, v) in enumerate(items, start=1):
                    label = day.isoformat() if hasattr(day, "isoformat") else str(day)
                    lines.append(f"{i}) {label} — {fmt_dur(v)}")

                # Reuse the normal period label when possible
                period = ""
                try:
                    if from_ymd and to_ymd and len(from_ymd) >= 10:
                        y, mo, _ = from_ymd.split("-", 2)
                        month_names = {
                            "01": "January",
                            "02": "February",
                            "03": "March",
                            "04": "April",
                            "05": "May",
                            "06": "June",
                            "07": "July",
                            "08": "August",
                            "09": "September",
                            "10": "October",
                            "11": "November",
                            "12": "December",
                        }
                        mm = mo.zfill(2)
                        if mm in month_names:
                            period = f" in {month_names[mm]} {y}"
                except Exception:
                    period = ""

                title = f"Time by day{period}"
                return {"reply": title + "\n" + "\n".join(lines), "diagnostics": {"matched": matched}}

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
                def fmt_dur(vmins: float) -> str:
                    mins = int(round(float(vmins)))
                    h, m = mins // 60, mins % 60
                    return f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")

                lines = []
                for i, (row, v) in enumerate(items, start=1):
                    label = row.strftime("%Y-%m") if hasattr(row, "strftime") else str(row)
                    lines.append(f"{i}) {label} — {fmt_dur(v)}")

                if plan.op == "top":
                    title = f"Top {len(items)} months by time"
                elif plan.op == "bottom":
                    title = f"Lowest {len(items)} months by time"
                else:
                    title = "Time by month"

                return {"reply": title + "\n" + "\n".join(lines), "diagnostics": {"matched": matched}}

            allowed = {"Team_Member": "team_member", "Client": "client", "Work": "work", "Role": "role", "Task_Type": "task_type", "Fee_Type": "fee_type"}
            col = allowed.get(gb, None)
            if not col:
                return {"reply": "I can only group by Team_Member/Client/Work/Role/Task_Type/Fee_Type/Month.", "diagnostics": {"matched": matched}}

            order = "ASC" if plan.op == "bottom" else "DESC"
            cur.execute(
                f"SELECT {col}, COALESCE(SUM({metric_col}),0) as v FROM {VIEW}{where_sql} GROUP BY {col} ORDER BY v {order} LIMIT %s",
                params + [plan.limit],
            )
            items = cur.fetchall()
            if not items:
                return {"reply": "I couldn’t find anything matching that in the database.", "diagnostics": {"matched": matched}}

            def fmt_dur(vmins: float) -> str:
                mins = int(round(float(vmins)))
                h, m = mins // 60, mins % 60
                return f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")

            def fmt_period() -> str:
                # Best-effort: convert from_ymd/to_ymd into a friendly label.
                try:
                    if from_ymd and to_ymd and len(from_ymd) >= 10:
                        try:
                            d0 = datetime.fromisoformat(from_ymd).date()
                            d1 = datetime.fromisoformat(to_ymd).date()
                            if (d1.toordinal() - d0.toordinal()) == 1:
                                return f" on {d0.isoformat()}"
                        except Exception:
                            pass

                        y, mo, _ = from_ymd.split("-", 2)
                        month_names = {
                            "01": "January",
                            "02": "February",
                            "03": "March",
                            "04": "April",
                            "05": "May",
                            "06": "June",
                            "07": "July",
                            "08": "August",
                            "09": "September",
                            "10": "October",
                            "11": "November",
                            "12": "December",
                        }
                        mm = mo.zfill(2)
                        if mm in month_names:
                            return f" in {month_names[mm]} {y}"
                except Exception:
                    pass
                return ""

            period = fmt_period()

            # If single result, return a natural sentence.
            if plan.limit == 1 and items:
                k, v = items[0]
                label = "(blank)" if k is None or (isinstance(k, str) and not k.strip()) else k
                dur = fmt_dur(v)
                if plan.op == "top":
                    if gb == "Client" and exclude_treewalk:
                        return {"reply": f"Apart from Treewalk, the client with the most hours{period} was {label} at {dur}.", "diagnostics": {"matched": matched}}
                    return {"reply": f"Most time{period} was {label} at {dur}.", "diagnostics": {"matched": matched}}
                if plan.op == "bottom":
                    return {"reply": f"Least time{period} was {label} at {dur}.", "diagnostics": {"matched": matched}}

            # Otherwise, return a ranked list.
            lines = []
            for i, (k, v) in enumerate(items, start=1):
                label = "(blank)" if k is None or (isinstance(k, str) and not k.strip()) else k
                lines.append(f"{i}) {label} — {fmt_dur(v)}")

            # Nicer title for day-specific breakdowns
            q = (raw_text or "").lower()
            if plan.op == "group_sum" and person and period.startswith(" on "):
                if gb == "Work" and ("what task" in q or "what tasks" in q):
                    title = f"{person} logged these tasks{period}"
                elif gb == "Task_Type" or ("breakdown" in q and "task" in q):
                    title = f"{person} logged these entries{period}"
                else:
                    title = f"{person} logged these entries{period}"
            else:
                if plan.op == "top":
                    title = f"Top {len(items)} by time{period}"
                elif plan.op == "bottom":
                    title = f"Lowest {len(items)} by time{period}"
                else:
                    title = f"Time by {gb}{period}"

            return {"reply": title + "\n" + "\n".join(lines), "diagnostics": {"matched": matched}}

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

            # Natural summary + keep raw data available for downstream steps
            reply_lines = [f"I found {len(data)} entries."]
            for i, r in enumerate(data[: min(len(data), 5)], start=1):
                # Compact line: date + client/work
                bits = []
                if r.get("Date"):
                    bits.append(r["Date"][:10])
                if r.get("Client"):
                    bits.append(str(r["Client"]))
                if r.get("Work"):
                    bits.append(str(r["Work"]))
                reply_lines.append(f"{i}) " + " — ".join(bits))
            if len(data) > 5:
                reply_lines.append(f"Plus {len(data) - 5} more.")

            return {"reply": "\n".join(reply_lines), "diagnostics": {"matched": matched}, "data": data}

        return {"reply": "Unsupported op.", "diagnostics": {"matched": matched}}
