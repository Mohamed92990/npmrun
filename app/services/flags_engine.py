from __future__ import annotations

from dataclasses import asdict
from datetime import date
import os

from app.services.postgres_client import PostgresClient, load_pg_conn_info
from app.services import rules_config as rc

# Use the raw landing table directly to avoid dependence on typed views.
TABLE = "public.karbon_timesheets"


def _ilike_any(col: str, keywords: list[str], params: list) -> str:
    """Build a SQL clause like: (col ILIKE %s OR col ILIKE %s ...)"""
    likes = []
    for kw in keywords:
        likes.append(f"{col} ILIKE %s")
        params.append(f"%{kw}%")
    return "(" + " OR ".join(likes) + ")" if likes else "(FALSE)"


def _norm_set(values: set[str]) -> set[str]:
    return {v.strip().lower() for v in values if (v or "").strip()}


def run_weekly_flags(*, from_ymd: str, to_ymd: str, limit: int = 50) -> dict:
    # Backward/forward compatible: some deployments require env_path param.
    # If Render env vars are set, postgres_client will ignore this file.
    conninfo = load_pg_conn_info(os.getenv("ENV_SUPABASE_PATH", ".env.supabase"))
    pg = PostgresClient(conninfo)

    fixed_fee = _norm_set(rc.FIXED_FEE_CLIENTS)

    flags: dict[str, list[dict]] = {
        "stat_holiday_missing_pto": [],
        "stat_holiday_extra_work": [],
        "tw_task_booked_to_client": [],
        "client_work_booked_to_treewalk": [],
        "fee_type_mismatch": [],
    }

    with pg.connect() as conn, conn.cursor() as cur:
        # --- Fee type mismatch (row-level)
        cur.execute(
            f"""
            SELECT "Date"::date, "Team Member", "Client", "Work", "Task Type", "Fee Type", COALESCE(NULLIF("Time (Minutes)", ''), '0')::numeric
            FROM {TABLE}
            WHERE "Date"::date >= %s::date AND "Date"::date < %s::date
              AND "Client" IS NOT NULL AND "Fee Type" IS NOT NULL
            """,
            [from_ymd, to_ymd],
        )
        # We’ll filter in Python for exact client list match (case-insensitive)
        rows = cur.fetchall()
        for d, tm, client, work, task_type, fee_type, mins in rows:
            expected = rc.FEE_TYPE_FIXED if (client or "").strip().lower() in fixed_fee else rc.FEE_TYPE_HOURLY
            actual = (fee_type or "").strip()
            # normalize fixed fee variations
            actual_norm = actual.lower()
            expected_norm = expected.lower()
            if ("fixed" in expected_norm and "fixed" not in actual_norm) or ("hour" in expected_norm and "hour" not in actual_norm):
                flags["fee_type_mismatch"].append(
                    {
                        "kind": "fee_type_mismatch",
                        "date": d.isoformat() if d else None,
                        "team_member": tm,
                        "client": client,
                        "work": work,
                        "task_type": task_type,
                        "fee_type": actual,
                        "minutes": float(mins or 0),
                        "details": f"Expected {expected}",
                    }
                )
                if len(flags["fee_type_mismatch"]) >= limit:
                    break

        # --- TW task booked to clients (or vice versa)
        # Do filtering in Python to avoid SQL placeholder duplication issues.
        cur.execute(
            f"""
            SELECT "Date"::date, "Team Member", "Client", "Work", "Task Type", "Fee Type", COALESCE(NULLIF("Time (Minutes)", ''), '0')::numeric
            FROM {TABLE}
            WHERE "Date"::date >= %s::date AND "Date"::date < %s::date
              AND "Task Type" IS NOT NULL
              AND "Client" IS NOT NULL
            ORDER BY "Date" DESC
            LIMIT %s
            """,
            [from_ymd, to_ymd, max(limit * 50, 2000)],
        )
        rows = cur.fetchall()
        for d, tm, client, work, task_type, fee_type, mins in rows:
            is_tw_task = any(kw in (task_type or "").lower() for kw in rc.TW_TASK_KEYWORDS)
            is_tw_client = any(kw in (client or "").lower() for kw in rc.TREEWALK_CLIENT_KEYWORDS)
            if is_tw_task and not is_tw_client:
                flags["tw_task_booked_to_client"].append(
                    {
                        "kind": "tw_task_booked_to_client",
                        "date": d.isoformat() if d else None,
                        "team_member": tm,
                        "client": client,
                        "work": work,
                        "task_type": task_type,
                        "fee_type": fee_type,
                        "minutes": float(mins or 0),
                    }
                )
            elif (not is_tw_task) and is_tw_client:
                flags["client_work_booked_to_treewalk"].append(
                    {
                        "kind": "client_work_booked_to_treewalk",
                        "date": d.isoformat() if d else None,
                        "team_member": tm,
                        "client": client,
                        "work": work,
                        "task_type": task_type,
                        "fee_type": fee_type,
                        "minutes": float(mins or 0),
                    }
                )

            if len(flags["tw_task_booked_to_client"]) >= limit and len(flags["client_work_booked_to_treewalk"]) >= limit:
                break

        # --- Stat holidays missing/extra PTO (person-day)
        # Determine which stat holidays fall inside the range.
        stat_days = [d for d in rc.BC_STAT_HOLIDAYS if from_ymd <= d < to_ymd]
        if stat_days:
            # PTO-coded row predicate across Task Type/Work/Notes
            pto_params: list = []
            pto_clause = "(" + " OR ".join(
                [
                    _ilike_any('"Task Type"', rc.PTO_KEYWORDS, pto_params),
                    _ilike_any('"Work"', rc.PTO_KEYWORDS, pto_params),
                    _ilike_any('COALESCE("Notes",\'\')', rc.PTO_KEYWORDS, pto_params),
                ]
            ) + ")"

            values_sql = ",".join(["(%s::date)" for _ in stat_days])

            cur.execute(
                f"""
                WITH stat_days(d) AS (VALUES {values_sql}),
                per_person_day AS (
                    SELECT
                      sd.d AS day,
                      t."Team Member" AS team_member,
                      SUM(CASE WHEN {pto_clause} THEN COALESCE(NULLIF(t."Time (Minutes)", ''), '0')::numeric ELSE 0 END) AS pto_minutes,
                      SUM(CASE WHEN {pto_clause} THEN 0 ELSE COALESCE(NULLIF(t."Time (Minutes)", ''), '0')::numeric END) AS work_minutes
                    FROM stat_days sd
                    LEFT JOIN {TABLE} t
                      ON t."Date"::date = sd.d
                     AND t."Date"::date >= %s::date AND t."Date"::date < %s::date
                    GROUP BY sd.d, t."Team Member"
                )
                SELECT day, team_member, pto_minutes, work_minutes
                FROM per_person_day
                WHERE team_member IS NOT NULL
                """,
                stat_days + pto_params + [from_ymd, to_ymd],
            )
            rows = cur.fetchall()
            for day, tm, pto_m, work_m in rows:
                pto_m = float(pto_m or 0)
                work_m = float(work_m or 0)
                if work_m > 0 and pto_m == 0:
                    flags["stat_holiday_extra_work"].append(
                        {
                            "kind": "stat_holiday_extra_work",
                            "date": day.isoformat() if day else None,
                            "team_member": tm,
                            "minutes": work_m,
                            "details": "Worked on stat holiday with no PTO recorded",
                        }
                    )
                elif work_m == 0 and pto_m == 0:
                    flags["stat_holiday_missing_pto"].append(
                        {
                            "kind": "stat_holiday_missing_pto",
                            "date": day.isoformat() if day else None,
                            "team_member": tm,
                            "minutes": 0,
                            "details": "No work and no PTO recorded on stat holiday",
                        }
                    )
                if len(flags["stat_holiday_extra_work"]) >= limit and len(flags["stat_holiday_missing_pto"]) >= limit:
                    break

    # Build a human-friendly summary
    def _count(k: str) -> int:
        return len(flags.get(k, []))

    reply_lines = [
        f"Flags for {from_ymd} to {to_ymd}.",
        f"Stat holiday: {_count('stat_holiday_extra_work')} extra-work, {_count('stat_holiday_missing_pto')} missing PTO.",
        f"TW miscoding: {_count('tw_task_booked_to_client')} TW→client, {_count('client_work_booked_to_treewalk')} client→TW.",
        f"Fee type mismatches: {_count('fee_type_mismatch') }.",
    ]

    return {
        "reply": "\n".join(reply_lines),
        "summary": {
            "from_ymd": from_ymd,
            "to_ymd": to_ymd,
            "counts": {k: len(v) for k, v in flags.items()},
        },
        "flags": flags,
    }
