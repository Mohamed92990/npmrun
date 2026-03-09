import os
from pathlib import Path

import requests
from dotenv import dotenv_values

ENV_PATH = Path(r"C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project\.env")


def main():
    cfg = dotenv_values(str(ENV_PATH))
    base = (cfg.get("KARBON_BASE_URL") or "").strip().rstrip("/")
    key = (cfg.get("KARBON_ACCESS_KEY") or "").strip()

    if not base:
        raise SystemExit("Missing KARBON_BASE_URL in .env")
    if not key:
        raise SystemExit("Missing KARBON_ACCESS_KEY in .env")

    url = f"{base}/v3/Users"

    tests = [
        ("Bearer only", {"Authorization": f"Bearer {key}"}),
        ("AccessKey only", {"AccessKey": key}),
        ("Bearer + AccessKey", {"Authorization": f"Bearer {key}", "AccessKey": key}),
    ]

    for name, headers in tests:
        print(f"\n== Test: {name} ==")
        try:
            r = requests.get(url, headers=headers, timeout=30)
            print("GET", url)
            print("status", r.status_code)
            ct = r.headers.get("content-type", "")
            print("content-type", ct)
            # Print a small snippet only
            text = r.text
            print("body (first 400 chars):")
            print(text[:400].replace("\n", "\\n"))
        except Exception as e:
            print("ERROR", e)


if __name__ == "__main__":
    main()
