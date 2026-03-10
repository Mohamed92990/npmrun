from __future__ import annotations

from pydantic import BaseModel, Field


class FlagsWeeklyIn(BaseModel):
    from_ymd: str = Field(..., description="Start date (inclusive), YYYY-MM-DD")
    to_ymd: str = Field(..., description="End date (exclusive), YYYY-MM-DD")
    limit: int = Field(50, ge=1, le=500)


class FlagItem(BaseModel):
    kind: str
    date: str | None = None
    team_member: str | None = None
    client: str | None = None
    work: str | None = None
    task_type: str | None = None
    fee_type: str | None = None
    minutes: float | None = None
    details: str | None = None


class FlagsWeeklyOut(BaseModel):
    reply: str
    summary: dict
    flags: dict[str, list[FlagItem]]
