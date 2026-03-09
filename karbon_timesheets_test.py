import requests
from dotenv import dotenv_values
from pathlib import Path

ENV_PATH = Path(r"C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project\.env")


def main():
    cfg = dotenv_values(str(ENV_PATH))
    base = (cfg.get("KARBON_BASE_URL") or "").strip().rstrip("/")
    access_key = (cfg.get("KARBON_ACCESS_KEY") or "").strip()
    bearer = (cfg.get("KARBON_BEARER_TOKEN") or "").strip()

    if not (base and access_key and bearer):
        raise SystemExit("Missing KARBON_BASE_URL/KARBON_ACCESS_KEY/KARBON_BEARER_TOKEN")

    url = f"{base}/v3/Timesheets?$top=5"
    headers = {
        "Authorization": f"Bearer {bearer}",
        "AccessKey": access_key,
        "Accept": "application/json",
    }

    r = requests.get(url, headers=headers, timeout=30)
    print("GET", url)
    print("status", r.status_code)
    print("content-type", r.headers.get("content-type", ""))
    print("body (first 1500 chars):")
    print(r.text[:1500].replace("\n", "\\n"))


if __name__ == "__main__":
    main()
