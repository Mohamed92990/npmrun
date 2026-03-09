import base64
import json
from pathlib import Path

import requests
from dotenv import dotenv_values

ENV_PATH = Path(r"C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project\.env")


def b64url_decode(s: str) -> bytes:
    s = s + "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s.encode())


def decode_jwt(token: str) -> dict:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        return json.loads(b64url_decode(parts[1]))
    except Exception:
        return {}


def try_get(url: str, headers: dict):
    r = requests.get(url, headers=headers, timeout=30)
    return r.status_code, r.headers.get("content-type", ""), r.text[:400]


def main():
    cfg = dotenv_values(str(ENV_PATH))
    base = (cfg.get("KARBON_BASE_URL") or "").strip().rstrip("/")
    key = (cfg.get("KARBON_ACCESS_KEY") or "").strip()
    if not base or not key:
        raise SystemExit("Missing KARBON_BASE_URL or KARBON_ACCESS_KEY")

    claims = decode_jwt(key)
    print("JWT claims (redacted):", {k: claims.get(k) for k in ("iss", "reg", "tak", "iat") if k in claims})

    endpoints = [
        "/v3/Users?$top=1",
        "/v3/Invoices?$top=1",
        "/v3/Payments?$top=1",
        "/v3/$metadata",
    ]

    header_sets = [
        ("Bearer", {"Authorization": f"Bearer {key}"}),
        ("Bearer + Accept", {"Authorization": f"Bearer {key}", "Accept": "application/json"}),
        ("Authorization=token", {"Authorization": key}),
        ("Authorization=JWT", {"Authorization": f"JWT {key}"}),
        ("Authorization=Token", {"Authorization": f"Token {key}"}),
        ("x-api-key", {"x-api-key": key}),
        ("X-Api-Key", {"X-Api-Key": key}),
        ("X-Access-Key", {"X-Access-Key": key}),
        ("AccessKey", {"AccessKey": key}),
        ("Bearer + AccessKey", {"Authorization": f"Bearer {key}", "AccessKey": key}),
    ]

    bases = [base]
    # try region-based variants if claim present
    reg = claims.get("reg")
    if isinstance(reg, str) and reg:
        bases += [
            f"https://{reg}.api.karbonhq.com",
            f"https://api-{reg}.karbonhq.com",
            f"https://api.karbonhq.com/{reg}",
        ]

    tested = 0
    for b in bases:
        for ep in endpoints:
            url = b.rstrip("/") + ep
            for name, headers in header_sets:
                tested += 1
                try:
                    code, ct, body = try_get(url, headers)
                except Exception as e:
                    continue
                if code != 401:
                    print("\n=== HIT ===")
                    print("base", b)
                    print("endpoint", ep)
                    print("headers", name)
                    print("status", code)
                    print("content-type", ct)
                    print("body", body.replace("\n", "\\n"))
                    return

    print("No non-401 response found. Tested combos:", tested)


if __name__ == "__main__":
    main()
