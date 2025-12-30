import requests
import csv
import os
import json

# Create folder to save CSV
os.makedirs("api_data", exist_ok=True)

def find_list_in_json(obj):
    """Recursively find first list of dicts inside JSON data."""
    if isinstance(obj, list):
        if all(isinstance(i, dict) for i in obj):
            return obj
        for i in obj:
            result = find_list_in_json(i)
            if result:
                return result
    elif isinstance(obj, dict):
        for v in obj.values():
            result = find_list_in_json(v)
            if result:
                return result
    return []

def normalize_value(v):
    """Flatten nested dicts/lists as JSON strings."""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    if v is None:
        return ""
    return str(v)

def fetch_json(url, key_name, filename):
    print(f"\nFetching data from: {url}")
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

    # Try key_name first
    records = []
    if isinstance(data, dict) and key_name in data:
        if isinstance(data[key_name], list):
            records = data[key_name]
        else:
            records = find_list_in_json(data[key_name])
    else:
        records = find_list_in_json(data)

    if not records:
        print(f"No list of records found for '{key_name}' in response.")
        print("Top-level keys:", list(data.keys()) if isinstance(data, dict) else type(data))
        return

    # Build headers
    headers = set()
    for r in records:
        if isinstance(r, dict):
            headers.update(r.keys())
    headers = list(headers)

    # Save CSV
    csv_path = os.path.join("api_data", f"{filename}.csv")
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for item in records:
                row = {k: normalize_value(item.get(k, "")) for k in headers}
                writer.writerow(row)
        print(f"Saved {len(records)} records to {csv_path}")
    except Exception as e:
        print(f"Error writing CSV: {e}")

# === TEST ON API === For no parameter API Which have full path
fetch_json(
    "https://www.bipm.org/api/kcdb/referenceData/radiationSource",
    key_name="referenceData",
    filename="radiationSource"
)


# /referenceData/radiationSource – Radiation source list   Request URL - https://www.bipm.org/api/kcdb/referenceData/radiationSource
# /referenceData/radiationMedium – Radiation medium list  Request URL - https://www.bipm.org/api/kcdb/referenceData/radiationMedium
# /referenceData/quantity – Quantity list   Request URL - https://www.bipm.org/api/kcdb/referenceData/quantity
# /referenceData/nuclide – Nuclide list   Request URL- https://www.bipm.org/api/kcdb/referenceData/nuclide
# /referenceData/domain – Domain list     Request URL - https://www.bipm.org/api/kcdb/referenceData/domain 
#/referenceData/country – Country list    Request URL - https://www.bipm.org/api/kcdb/referenceData/country
# /referenceData/category – Category list    Request URL - https://www.bipm.org/api/kcdb/referenceData/category
# /referenceData/analyte – Analyte list     Request URL -https://www.bipm.org/api/kcdb/referenceData/analyte