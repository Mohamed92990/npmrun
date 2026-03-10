from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException

from app.core.config import WEBHOOK_SECRET
from app.models.flags import FlagsWeeklyIn, FlagsWeeklyOut, FlagItem
from app.services.flags_engine import run_weekly_flags

router = APIRouter()


@router.post("/flags/weekly", response_model=FlagsWeeklyOut)
def flags_weekly(payload: FlagsWeeklyIn, x_webhook_secret: str | None = Header(default=None)):
    if WEBHOOK_SECRET and x_webhook_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Missing/invalid X-Webhook-Secret")

    try:
        result = run_weekly_flags(
            from_ymd=payload.from_ymd,
            to_ymd=payload.to_ymd,
            limit=payload.limit,
        )

        return FlagsWeeklyOut(
            reply=result["reply"],
            summary=result["summary"],
            flags={k: [FlagItem(**x) for x in v] for k, v in result["flags"].items()},
        )
    except Exception as e:
        # Return a safe, actionable error message for n8n without leaking secrets.
        msg = f"flags_weekly failed: {type(e).__name__}: {str(e)}"
        msg = msg.replace("password", "[redacted]")
        if len(msg) > 400:
            msg = msg[:400] + "..."
        raise HTTPException(status_code=500, detail=msg)
