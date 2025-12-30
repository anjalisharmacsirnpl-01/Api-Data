# fetch_branches_1to7.py
import requests
import csv
import json
import os
import time
from typing import Any, Dict, List
Domin_IDS = ["CHEM-BIO", "PHYSICS","RADIATION"]
OUT_DIR = "api_data"
COMBINED_CSV = "branches_areas_1_7.csv"
HEADERS = {
    "Accept": "application/json, application/*+json",
    "User-Agent": "python-requests/fetch-branches-script"
}
TIMEOUT = 15
SLEEP = 0.2  # polite pause

os.makedirs(OUT_DIR, exist_ok=True)

def find_list_in_json(obj: Any) -> List[Dict]:
    if isinstance(obj, list):
        if all(isinstance(i, dict) for i in obj):
            return obj
        for i in obj:
            r = find_list_in_json(i)
            if r:
                return r
    elif isinstance(obj, dict):
        for v in obj.values():
            r = find_list_in_json(v)
            if r:
                return r
    return []

def normalize_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)

def fetch_metrologyArea(area_id) -> List[Dict]:
    url = "https://www.bipm.org/api/kcdb/referenceData/metrologyArea"
    try:
        r = requests.get(url, params={"domainCode": area_id}, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"[area {area_id}] Request failed: {e}")
        return []
    try:
        data = r.json()
    except ValueError:
        print(f"[area {area_id}] Non-JSON response (content-type: {r.headers.get('Content-Type')})")
        return []
    recs = find_list_in_json(data)
    return [r for r in recs if isinstance(r, dict)]

# def fetch_branches(area_id: int) -> List[Dict]:
#     url = "https://www.bipm.org/api/kcdb/referenceData/branch"
#     try:
#         r = requests.get(url, params={"areaId": area_id}, headers=HEADERS, timeout=TIMEOUT)
#         r.raise_for_status()
#     except requests.RequestException as e:
#         print(f"[area {area_id}] Request failed: {e}")
#         return []
#     try:
#         data = r.json()
#     except ValueError:
#         print(f"[area {area_id}] Non-JSON response (content-type: {r.headers.get('Content-Type')})")
#         return []
#     recs = find_list_in_json(data)
#     return [r for r in recs if isinstance(r, dict)]

def write_csv(path: str, records: List[Dict]):
    if not records:
        print(f"No records to write for {path}")
        return
    keys = set()
    for r in records:
        keys.update(r.keys())
    fieldnames = list(keys)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            row = {k: normalize_value(r.get(k, "")) for k in fieldnames}
            writer.writerow(row)
    print(f"WROTE {len(records)} rows -> {path}")

def main():
    all_records = []
    for aid in Domin_IDS:
        print(f"Fetching areaId={aid} ...")
        recs = fetch_metrologyArea(aid)
        print(f"  returned {len(recs)} records")
        # attach area metadata
        for r in recs:
            r["_areaId"] = str(aid)
        # save per-area csv (optional)
        per_path = os.path.join(OUT_DIR, f"branch_area_{aid}.csv")
        write_csv(per_path, recs)
        all_records.extend(recs)
        time.sleep(SLEEP)

    # deduplicate by JSON content
    unique = []
    seen = set()
    for rec in all_records:
        key = json.dumps(rec, sort_keys=True, ensure_ascii=False)
        if key not in seen:
            seen.add(key)
            unique.append(rec)

    combined_path = os.path.join(OUT_DIR, COMBINED_CSV)
    write_csv(combined_path, unique)
    print(f"\nTOTAL unique records (areas {Domin_IDS}): {len(unique)}")
    print("Done.")

if __name__ == "__main__":
    main()