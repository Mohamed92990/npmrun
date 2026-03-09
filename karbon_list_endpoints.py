import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from dotenv import dotenv_values

ENV_PATH = Path(r"C:\Users\sayyi\Desktop\Karbon Timesheets Automation Project\.env")


def main():
    cfg = dotenv_values(str(ENV_PATH))
    base = (cfg.get("KARBON_BASE_URL") or "").strip().rstrip("/")
    access_key = (cfg.get("KARBON_ACCESS_KEY") or "").strip()
    bearer = (cfg.get("KARBON_BEARER_TOKEN") or "").strip()

    if not (base and access_key and bearer):
        raise SystemExit("Missing KARBON_BASE_URL/KARBON_ACCESS_KEY/KARBON_BEARER_TOKEN")

    url = f"{base}/v3/$metadata"
    headers = {"Authorization": f"Bearer {bearer}", "AccessKey": access_key, "Accept": "application/xml"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    xml = r.text

    # Parse OData metadata (EDMX). We only extract EntitySets (roughly the top-level collections).
    ns = {
        "edmx": "http://docs.oasis-open.org/odata/ns/edmx",
        "edm": "http://docs.oasis-open.org/odata/ns/edm",
    }
    root = ET.fromstring(xml)

    entity_sets = []
    for es in root.findall(".//edm:EntityContainer/edm:EntitySet", ns):
        name = es.attrib.get("Name")
        etype = es.attrib.get("EntityType")
        entity_sets.append((name, etype))

    singletons = []
    for s in root.findall(".//edm:EntityContainer/edm:Singleton", ns):
        name = s.attrib.get("Name")
        stype = s.attrib.get("Type")
        singletons.append((name, stype))

    print(f"Metadata OK. EntitySets: {len(entity_sets)}")
    for name, etype in sorted(entity_sets):
        print(f"- GET /v3/{name}   ({etype})")

    if singletons:
        print(f"\nSingletons: {len(singletons)}")
        for name, stype in sorted(singletons):
            print(f"- GET /v3/{name}   ({stype})")


if __name__ == "__main__":
    main()
