import json
import time
import requests

BASE = "http://127.0.0.1:8031"

QUERIES = [
    "in the month of February 2026 which client had the most hours",
    "Apart from Treewalk which client had the most hours in the month of February 2026",
    "How many hours did we spend on Nemo Resources Inc. in February",
    "How many hours did we spend on Nemo Resources Inc. in February 2026",
]


def post_query(text: str, timeout_s: int = 90):
    r = requests.post(
        f"{BASE}/v1/query",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"text": text}),
        timeout=timeout_s,
    )
    return r


def main():
    for _ in range(30):
        try:
            r = requests.get(f"{BASE}/healthz", timeout=2)
            if r.status_code == 200:
                break
        except Exception:
            time.sleep(0.5)

    for q in QUERIES:
        print("\n===", q, "===")
        t0 = time.time()
        r = post_query(q)
        dt = time.time() - t0
        print(f"HTTP {r.status_code} in {dt:.2f}s")
        try:
            print((r.json().get("reply") or "").strip())
        except Exception:
            print(r.text[:2000])


if __name__ == "__main__":
    main()
