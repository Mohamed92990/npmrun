from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import re

from app.core.config import ALLOWED_FIELDS
from app.models.query_plan import QueryPlan
from app.services.airtable_client import AirtableClient


def norm(s: str) -> str:
    return " ".join("".join(ch.lower() if ch.isalnum() or ch.isspace() else " " for ch in (s or "")).split())


def parse_ymd(ds) -> str | None:
    if not ds:
        return None
    try:
        s = str(ds).replace("Z", "+00:00")
        d = datetime.fromisoformat(s)
        return d.date().strftime("%Y-%m-%d")
    except Exception:
        return None


MONTHS = {
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


def month_range_ymd(month: int, year: int):
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)
    return start.date().strftime("%Y-%m-%d"), end.date().strftime("%Y-%m-%d")


def most_recent_year_for_month(month: int, records: list[dict]) -> int | None:
    years = set()
    for rec in records:
        ymd = parse_ymd((rec.get("fields") or {}).get("Date"))
        if not ymd:
            continue
        try:
            d = datetime.fromisoformat(ymd)
        except Exception:
            continue
        if d.month == month:
            years.add(d.year)
    return max(years) if years else None


def infer_month_range_from_text(text: str, records: list[dict]) -> tuple[str | None, str | None]:
    t = (text or "").lower()
    month = None
    for name, num in MONTHS.items():
        if name in t:
            month = num
            break
    if not month:
        return None, None

    # If year present, use it
    m = re.search(r"\b(20\d{2})\b", t)
    if m:
        yr = int(m.group(1))
        return month_range_ymd(month, yr)

    # Otherwise, choose most recent year in DB for that month
    yr = most_recent_year_for_month(month, records)
    if not yr:
        return None, None
    return month_range_ymd(month, yr)


def execute_plan(plan: QueryPlan, raw_text: str = "") -> dict:
    at = AirtableClient()
    records = at.fetch_records(max_records=5000)

    # normalize empty strings to None
    plan_person = (plan.person or "").strip() or None
    plan_client = (plan.client or "").strip() or None
    plan_work = (plan.work or "").strip() or None
    plan_role = (plan.role or "").strip() or None
    plan_task = (plan.task_type or "").strip() or None

    from_ymd = (plan.from_ymd or "").strip() or None
    to_ymd = (plan.to_ymd or "").strip() or None

    # If no date range provided but the text contains a month, infer month range.
    if not (from_ymd and to_ymd):
        inferred_from, inferred_to = infer_month_range_from_text(raw_text, records)
        from_ymd = from_ymd or inferred_from
        to_ymd = to_ymd or inferred_to

    def row_ok(f: dict) -> bool:
        if plan_person and plan_person.lower() not in str(f.get("Team_Member", "")).lower():
            return False
        if plan_client and norm(plan_client) != norm(str(f.get("Client", ""))):
            return False
        if plan_work and norm(plan_work) != norm(str(f.get("Work", ""))):
            return False
        if plan_role and norm(plan_role) not in norm(str(f.get("Role", ""))):
            return False
        if plan_task and norm(plan_task) not in norm(str(f.get("Task_Type", ""))):
            return False

        if from_ymd and to_ymd:
            ymd = parse_ymd(f.get("Date"))
            if not ymd:
                return False
            if not (from_ymd <= ymd < to_ymd):
                return False

        return True

    filtered = [r for r in records if row_ok(r.get("fields", {}))]

    def metric_value(f: dict) -> float:
        if plan.metric == "cost":
            try:
                return float(f.get("Cost") or 0)
            except Exception:
                return 0.0
        # default time_minutes
        try:
            return float(int(f.get("Time_Minutes") or 0))
        except Exception:
            return 0.0

    # op handlers
    if plan.op == "sum":
        total = sum(metric_value(r.get("fields", {})) for r in filtered)
        if plan.metric == "cost":
            reply = f"Total cost is {total:.2f}."
            return {"reply": reply, "diagnostics": {"matched": len(filtered), "total_cost": total}}
        mins = int(round(total))
        h, m = mins // 60, mins % 60
        dur = f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")
        reply = f"Total time is {dur}."
        return {"reply": reply, "diagnostics": {"matched": len(filtered), "total_minutes": mins}}

    if plan.op in ("distinct",):
        if not plan.group_by:
            return {"reply": "I need group_by for distinct.", "diagnostics": {"matched": len(filtered)}}
        if plan.group_by not in ALLOWED_FIELDS:
            return {"reply": "That field is not allowed.", "diagnostics": {"matched": len(filtered)}}
        vals = sorted({str((r.get("fields", {}) or {}).get(plan.group_by, "")).strip() for r in filtered if str((r.get("fields", {}) or {}).get(plan.group_by, "")).strip()})
        if not vals:
            return {"reply": "I couldn’t find anything matching that in the database.", "diagnostics": {"matched": len(filtered)}}
        return {"reply": f"{plan.group_by} values: " + ", ".join(vals[: plan.limit]), "diagnostics": {"matched": len(filtered), "count": len(vals)}}

    if plan.op in ("group_sum", "top"):
        if not plan.group_by:
            return {"reply": "I need group_by for grouping.", "diagnostics": {"matched": len(filtered)}}
        if plan.group_by not in ALLOWED_FIELDS:
            return {"reply": "That field is not allowed.", "diagnostics": {"matched": len(filtered)}}
        buckets = defaultdict(float)
        for r in filtered:
            f = r.get("fields", {})
            k = str(f.get(plan.group_by, "")).strip() or "(blank)"
            buckets[k] += metric_value(f)
        items = sorted(buckets.items(), key=lambda kv: kv[1], reverse=True)[: plan.limit]

        if plan.metric == "cost":
            lines = [f"- {k}: {v:.2f}" for k, v in items]
            title = f"Top {len(items)} by cost" if plan.op == "top" else f"Cost by {plan.group_by}"
        else:
            lines = []
            for k, v in items:
                mins = int(round(v))
                h, m = mins // 60, mins % 60
                dur = f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")
                lines.append(f"- {k}: {dur}")
            title = f"Top {len(items)} by time" if plan.op == "top" else f"Time by {plan.group_by}"

        return {"reply": title + ":\n" + "\n".join(lines), "diagnostics": {"matched": len(filtered)}}

    if plan.op == "list":
        fields = plan.fields or ["Date", "Team_Member", "Client", "Work", "Time_Minutes", "Cost"]
        fields = [f for f in fields if f in ALLOWED_FIELDS]
        data = []
        for r in filtered[: plan.limit]:
            f = r.get("fields", {})
            data.append({k: f.get(k) for k in fields})
        if not data:
            return {"reply": "I couldn’t find anything matching that in the database.", "diagnostics": {"matched": 0}, "data": []}
        return {"reply": f"Here are {len(data)} records.", "diagnostics": {"matched": len(filtered)}, "data": data}

    return {"reply": "Unsupported op.", "diagnostics": {"matched": len(filtered)}}
