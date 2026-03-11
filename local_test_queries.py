import json
import time
import requests

BASE = "http://127.0.0.1:8030"

QUERIES = [
    "How many hours did Akash work on January 2nd 2026?",
    "How many hours did Akash work on January 5th 2026?",
    "How many hours did Akash work on 2026-01-02?",
    "How many hours did Akash work on Jan 2, 2026?",
    "What tasks did Jay work on Feb 2nd 2026",
    "What tasks did Jay work on 2026-02-02",
    "In January 2026 give me the top 5 clients we spent most hours on",
    "How many non billable hours did Akash have in January 2026?",
]


def post_query(text: str, timeout_s: int = 60):
    r = requests.post(
        f"{BASE}/v1/query",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"text": text}),
        timeout=timeout_s,
    )
    return r


def main():
    # wait for server
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
        try:
            r = post_query(q, timeout_s=90)
            dt = time.time() - t0
            print(f"HTTP {r.status_code} in {dt:.2f}s")
            try:
                j = r.json()
                print((j.get("reply") or "").strip())
            except Exception:
                print(r.text[:2000])
        except Exception as e:
            dt = time.time() - t0
            print(f"ERROR after {dt:.2f}s: {e}")


if __name__ == "__main__":
    main()
