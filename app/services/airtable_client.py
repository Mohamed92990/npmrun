from __future__ import annotations

from typing import Optional

import requests

from app.core.config import AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID, AIRTABLE_TOKEN, AIRTABLE_VIEW_ID


class AirtableClient:
    def __init__(self):
        if not (AIRTABLE_TOKEN and AIRTABLE_BASE_ID and AIRTABLE_TABLE_ID):
            raise RuntimeError("Missing Airtable config in .env")
        self.base_id = AIRTABLE_BASE_ID
        self.table_id = AIRTABLE_TABLE_ID
        self.view_id = AIRTABLE_VIEW_ID
        self.headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}

    def fetch_records(
        self,
        *,
        page_size: int = 100,
        max_records: int = 5000,
        view: Optional[str] = None,
        filter_by_formula: Optional[str] = None,
    ) -> list[dict]:
        if page_size > 100:
            page_size = 100

        url = f"https://api.airtable.com/v0/{self.base_id}/{self.table_id}"
        out: list[dict] = []
        offset = None

        while True:
            params = {"pageSize": page_size}
            v = view if view is not None else self.view_id
            if v:
                params["view"] = v
            if filter_by_formula:
                params["filterByFormula"] = filter_by_formula
            if offset:
                params["offset"] = offset

            r = requests.get(url, headers=self.headers, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()

            out.extend(data.get("records", []))
            if len(out) >= max_records:
                return out[:max_records]

            offset = data.get("offset")
            if not offset:
                return out
