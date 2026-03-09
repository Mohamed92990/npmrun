import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path

CSV_PATH = Path(r"C:\Users\sayyi\.openclaw\media\inbound\file_1---c6ea1c13-932b-46c7-8a99-1344a2bd1ed2.csv")


def ymd(s: str) -> str:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()


def load():
    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def sum_minutes(rows, *, person=None, client=None, from_ymd=None, to_ymd=None, task_contains=None):
    total = 0
    matched = 0
    for r in rows:
        if person and r.get("Team_Member") != person:
            continue
        if client and r.get("Client") != client:
            continue
        d = ymd(r["Date"])
        if from_ymd and to_ymd and not (from_ymd <= d < to_ymd):
            continue
        if task_contains and task_contains.lower() not in (r.get("Task_Type") or "").lower():
            continue
        try:
            total += int(float(r.get("Time_Minutes") or 0))
        except Exception:
            pass
        matched += 1
    return total, matched


def sum_cost(rows, *, client=None, from_ymd=None, to_ymd=None):
    total = 0.0
    matched = 0
    for r in rows:
        if client and r.get("Client") != client:
            continue
        d = ymd(r["Date"])
        if from_ymd and to_ymd and not (from_ymd <= d < to_ymd):
            continue
        try:
            total += float(r.get("Cost") or 0)
        except Exception:
            pass
        matched += 1
    return total, matched


def distinct_people(rows, *, client=None, from_ymd=None, to_ymd=None, task_contains=None):
    people = set()
    matched = 0
    for r in rows:
        if client and r.get("Client") != client:
            continue
        d = ymd(r["Date"])
        if from_ymd and to_ymd and not (from_ymd <= d < to_ymd):
            continue
        if task_contains and task_contains.lower() not in (r.get("Task_Type") or "").lower():
            continue
        tm = (r.get("Team_Member") or "").strip()
        if tm:
            people.add(tm)
        matched += 1
    return sorted(people), matched


def top_clients_by_minutes(rows, *, from_ymd=None, to_ymd=None, limit=5):
    buckets = defaultdict(int)
    matched = 0
    for r in rows:
        d = ymd(r["Date"])
        if from_ymd and to_ymd and not (from_ymd <= d < to_ymd):
            continue
        try:
            mins = int(float(r.get("Time_Minutes") or 0))
        except Exception:
            mins = 0
        buckets[r.get("Client") or "(blank)"] += mins
        matched += 1
    items = sorted(buckets.items(), key=lambda kv: kv[1], reverse=True)[:limit]
    return items, matched


def main():
    rows = load()

    # 1) Tiny Boards LP on Jan 2 2025
    people, matched = distinct_people(rows, client="Tiny Boards LP", from_ymd="2025-01-02", to_ymd="2025-01-03")
    print("1) People Tiny Boards LP on 2025-01-02:", people, "matched", matched)

    # 2) Akash Brar January 2025
    mins, matched = sum_minutes(rows, person="Akash Brar", from_ymd="2025-01-01", to_ymd="2025-02-01")
    print("2) Akash Brar minutes Jan 2025:", mins, "matched", matched)

    # 3) Atomic cost January 2025
    cost, matched = sum_cost(rows, client="Atomic Development Inc.", from_ymd="2025-01-01", to_ymd="2025-02-01")
    print("3) Atomic cost Jan 2025:", cost, "matched", matched)

    # 4) Top 5 clients by minutes Jan 2025
    items, matched = top_clients_by_minutes(rows, from_ymd="2025-01-01", to_ymd="2025-02-01", limit=5)
    def fmt(mins: int):
        h, m = mins // 60, mins % 60
        return f"{h}h {m}m" if h and m else (f"{h}h" if h else f"{m}m")
    print("4) Top 5 clients Jan 2025:")
    for c, mins in items:
        print("  -", c, fmt(mins))
    print("   matched", matched)

    # 5) Bookkeeping people for Bayridge Jan 2025
    people, matched = distinct_people(rows, client="Bayridge Resources Corp.", from_ymd="2025-01-01", to_ymd="2025-02-01", task_contains="Bookkeeping")
    print("5) Bayridge bookkeeping people Jan 2025:", people, "matched", matched)


if __name__ == "__main__":
    main()
