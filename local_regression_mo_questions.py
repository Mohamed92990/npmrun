import json
import time
import requests

BASE = "http://127.0.0.1:8040"

QUESTIONS = [
    "What bookkeeping did Ubaid do in January 2026?",
    "Give me a break down of the tasks types Ubaid did in January 2026",
    "What clients did Ubaid book the task type Bookkeeping:Continous Bookeeping on in January 2026?",
    "What clients did Ubaid book the task type \"Bookkeeping: Continuous bookkeeping\" on in January 2026?",
    "What clients did Ubaid book the task type \"Bookkeeping: Continuous bookkeeping\" on in January 2026 and how much time was spent on each?",
    "Did Ubaid book PTO time in February 2026?",
    "What date(s) did Ubaid book PTO in February 2026?",
    "Did Nicholas attend any huddles in January?",
    "Did Jay attend any huddles in January 2026?",
    "What days and for how much time did Jay attend huddles in January 2026?",
    "How much time was spent on Neotech Metals Corp. (formerly Caravan) in January and February 2026?",
]


def wait_ready():
    for _ in range(50):
        try:
            r = requests.get(f"{BASE}/healthz", timeout=2)
            if r.status_code == 200:
                return True
        except Exception:
            time.sleep(0.2)
    return False


def main():
    assert wait_ready(), "server not ready"
    for q in QUESTIONS:
        print("\n===", q, "===")
        t0 = time.time()
        r = requests.post(f"{BASE}/v1/query", json={"text": q}, timeout=90)
        dt = time.time() - t0
        print(f"HTTP {r.status_code} in {dt:.2f}s")
        try:
            print((r.json().get("reply") or "").strip())
        except Exception:
            print(r.text[:2000])


if __name__ == "__main__":
    main()
