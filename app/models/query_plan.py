from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


Metric = Literal["time_minutes", "cost"]
Op = Literal["sum", "distinct", "group_sum", "top", "bottom", "list", "percent"]


class QueryPlan(BaseModel):
    # high-level
    op: Op
    metric: Optional[Metric] = None
    group_by: Optional[str] = None
    fields: Optional[list[str]] = None
    limit: int = Field(default=10, ge=1, le=200)

    @field_validator("limit", mode="before")
    @classmethod
    def coerce_limit(cls, v):
        # LLM sometimes emits 0; clamp to a sane default.
        try:
            n = int(v)
        except Exception:
            return 10
        return 10 if n < 1 else (200 if n > 200 else n)

    @field_validator("metric", "group_by", "person", "client", "work", "role", "task_type", "fee_type", "from_ymd", "to_ymd", mode="before")
    @classmethod
    def empty_str_to_none(cls, v):
        if isinstance(v, str) and not v.strip():
            return None
        return v

    # filters
    person: Optional[str] = None  # Team_Member substring
    client: Optional[str] = None
    work: Optional[str] = None
    role: Optional[str] = None
    task_type: Optional[str] = None
    fee_type: Optional[str] = None  # e.g. "Billable", "Non-Billable" (filter by Fee_Type)

    # date filters
    from_ymd: Optional[str] = Field(default=None, description="YYYY-MM-DD inclusive")
    to_ymd: Optional[str] = Field(default=None, description="YYYY-MM-DD exclusive")


class NLQueryIn(BaseModel):
    text: str
    user: Optional[str] = None
    channel: Optional[str] = None


class QueryOut(BaseModel):
    reply: str
    plan: QueryPlan
    diagnostics: dict = {}
    data: Optional[object] = None
