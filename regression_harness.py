import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests

CSV_PATH = Path(r"C:\Users\sayyi\.openclaw\media\inbound\file_1---c6ea1c13-932b-46c7-8a99-1344a2bd1ed2.csv")
API_URL = "http://127.0.0.1:8001/v1/query"


def ymd_from_iso(s: str) -> str:
    s = s.replace("Z", "+00:00")
    d = datetime.fromisoformat(s)
    return d.date().strftime("%Y-%m-%d")


def load_rows():
    rows = []
    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


def people_for_client_on_day(rows, client, day_ymd):
    people = set()
    for r in rows:
        if r.get("Client") != client:
            continue
        if ymd_from_iso(r["Date"]) != day_ymd:
            continue
        tm = (r.get("Team_Member") or "").strip()
        if tm:
            people.add(tm)
    return sorted(people)


def sum_cost_for_client_on_day(rows, client, day_ymd):
    total = 0.0
    for r in rows:
        if r.get("Client") != client:
            continue
        if ymd_from_iso(r["Date"]) != day_ymd:
            continue
        try:
            total += float(r.get("Cost") or 0)
        except Exception:
            pass
    return total


def sum_minutes_for_person_client_month(rows, person, client, month, year):
    total = 0
    for r in rows:
        if r.get("Team_Member") != person:
            continue
        if r.get("Client") != client:
            continue
        d = datetime.fromisoformat(r["Date"].replace("Z", "+00:00"))
        if d.year != year or d.month != month:
            continue
        try:
            total += int(float(r.get("Time_Minutes") or 0))
        except Exception:
            pass
    return total


def call_api(q: str):
    r = requests.post(API_URL, json={"text": q, "user": "Mo", "channel": "regression"}, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    rows = load_rows()

    tests = []

    # Test 1: people on client/day
    client = "Atomic Development Inc."
    day = "2025-01-02"
    exp_people = people_for_client_on_day(rows, client, day)
    q1 = "Who worked on Atomic Development Inc. on January 2nd 2025?"
    tests.append((q1, {"expect_people": exp_people}))

    # Test 2: cost sum on client/day
    client2 = "Tiny Boards LP"
    day2 = "2025-01-02"
    exp_cost = sum_cost_for_client_on_day(rows, client2, day2)
    q2 = "Total cost for Tiny Boards LP on January 2nd 2025"
    tests.append((q2, {"expect_cost": exp_cost}))

    # Test 3: hours (minutes) for person+client+month
    exp_min = sum_minutes_for_person_client_month(rows, "Akash Brar", "Tiny Boards LP", 1, 2025)
    q3 = "How many hours did Akash Brar work for Tiny Boards LP in January 2025?"
    tests.append((q3, {"expect_minutes": exp_min}))

    failures = 0
    for q, exp in tests:
        out = call_api(q)
        print("\nQ:", q)
        print("Reply:", out.get("reply"))
        print("Plan:", json.dumps(out.get("plan"), indent=2))
        print("Diagnostics:", out.get("diagnostics"))
        
        # basic checks
        if "expect_people" in exp:
            # naive: ensure all expected names appear in reply string
            missing = [p for p in exp["expect_people"] if p not in (out.get("reply") or "")]
            if missing:
                failures += 1
                print("FAIL missing people in reply:", missing, "expected:", exp["expect_people"])
        if "expect_cost" in exp:
            # extract float from reply
            import re

            m = re.search(r"([0-9]+\.[0-9]+)", out.get("reply") or "")
            got = float(m.group(1)) if m else None
            if got is None or abs(got - exp["expect_cost"]) > 0.01:
                failures += 1
                print("FAIL cost", got, "expected", exp["expect_cost"])
        if "expect_minutes" in exp:
            # check diagnostics total_minutes if present
            mins = None
            diag = out.get("diagnostics") or {}
            mins = diag.get("total_minutes")
            if mins is None:
                # try parse from reply
                mins = None
            if mins is None or int(mins) != int(exp["expect_minutes"]):
                failures += 1
                print("FAIL minutes", mins, "expected", exp["expect_minutes"])

    print("\nFailures:", failures)
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
