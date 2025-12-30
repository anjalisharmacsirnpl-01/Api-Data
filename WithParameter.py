import requests
import csv
import os
import json
import time

# Create folder to save CSV files
os.makedirs("api_data", exist_ok=True)

HEADERS = {
    "Accept": "application/json, application/*+json",
    "User-Agent": "python-requests/fetch-individual-service"
}

def find_list_in_json(obj):
    """Recursively find first list of dicts in JSON."""
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
    """Flatten nested objects as JSON strings."""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    if v is None:
        return ""
    return str(v)

#===========================#
#    SubService Table       #
#===========================#

# def fetch_json(serviceId):
#     """Fetch data for one branchId."""
#     url = "https://www.bipm.org/api/kcdb/referenceData/subService"
#     params = {"serviceId": serviceId}
#     try:
#         response = requests.get(url, headers=HEADERS, params=params, timeout=10)
#         if response.status_code != 200:
#             return None
#         data = response.json()
#         records = data.get("referenceData", []) or find_list_in_json(data)
#         if records:
#             return records
#         return None
#     except Exception as e:
#         print(f"Error fetching ID {serviceId}: {e}")
#         return None

#==========================#
#       Service Table      #
#==========================#

# def fetch_json(branchId):
#     """Fetch data for one branchId."""
#     url = "https://www.bipm.org/api/kcdb/referenceData/service"
#     params = {"branchId": branchId}
#     try:
#         response = requests.get(url, headers=HEADERS, params=params, timeout=10)
#         if response.status_code != 200:
#             return None
#         data = response.json()
#         records = data.get("referenceData", []) or find_list_in_json(data)
#         if records:
#             return records
#         return None
#     except Exception as e:
#         print(f"Error fetching ID {branchId}: {e}")
#         return None

#=============================#
#   Individual Service Table  #
#=============================#

def fetch_json(subServiceId):
    """Fetch data for one subServiceId."""
    url = "https://www.bipm.org/api/kcdb/referenceData/individualService"
    params = {"subServiceId": subServiceId}
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=10)
        if response.status_code != 200:
            return None
        data = response.json()
        records = data.get("referenceData", []) or find_list_in_json(data)
        if records:
            return records
        return None
    except Exception as e:
        print(f"Error fetching ID {subServiceId}: {e}")
        return None


#=============================#
#        Branch Table         #
#=============================#

# def fetch_json(areaId):
#     """Fetch data for one areaId."""
#     url = "https://www.bipm.org/api/kcdb/referenceData/branch"
#     params = {"areaId": areaId}
#     try:
#         response = requests.get(url, headers=HEADERS, params=params, timeout=10)
#         if response.status_code != 200:
#             return None
#         data = response.json()
#         records = data.get("referenceData", []) or find_list_in_json(data)
#         if records:
#             return records
#         return None
#     except Exception as e:
#         print(f"Error fetching ID {areaId}: {e}")
#         return None
    

def save_to_csv(records, filename):
    """Save a list of dicts to CSV."""
    if not records:
        return
    keys = set()
    for r in records:
        keys.update(r.keys())
    headers = list(keys)

    path = os.path.join("api_data", filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in records:
            row = {k: normalize_value(r.get(k, "")) for k in headers}
            writer.writerow(row)
    print(f"Saved {len(records)} records → {path}")

def main():
    all_records = []
    valid_ids = []


#===========================#
#    SubService Table       #
#===========================#

    # for i in range(1, 300):
    #     print(f"Fetching serviceId={i} ...", end=" ")
    #     records = fetch_json(i)
    #     if records:
    #         print(f"found {len(records)} records")
    #         for r in records:
    #             r["_serviceId"] = i  # tag the source
    #         valid_ids.append(i)
    #         all_records.extend(records)
    #         save_to_csv(records, f"ServiceID_{i}.csv")
    #     else:
    #         print("no data")
    #     time.sleep(0.2)  # polite delay between requests


#==========================#
#       Service Table      #
#==========================#


    # for i in range(1, 300):
    #     print(f"Fetching branchId={i} ...", end=" ")
    #     records = fetch_json(i)
    #     if records:
    #         print(f"found {len(records)} records")
    #         for r in records:
    #             r["_branchId"] = i  # tag the source
    #         valid_ids.append(i)
    #         all_records.extend(records)
    #         save_to_csv(records, f"branchId_{i}.csv")
    #     else:
    #         print("no data")
    #     time.sleep(0.2)  # polite delay between requests


#=============================#
#   Individual Service Table  #
#=============================#

    for i in range(1, 501):
        print(f"Fetching subServiceId={i} ...", end=" ")
        records = fetch_json(i)
        if records:
            print(f"found {len(records)} records")
            for r in records:
                r["_subServiceId"] = i  # tag the source
            valid_ids.append(i)
            all_records.extend(records)
            save_to_csv(records, f"SubServiceID_{i}.csv")
        else:
            print("no data")
        time.sleep(0.2)  # polite delay between requests

#=============================#
#        Branch Table         #
#=============================#

    # for i in range(1, 300):
    #     print(f"Fetching areaId={i} ...", end=" ")
    #     records = fetch_json(i)
    #     if records:
    #         print(f"found {len(records)} records")
    #         for r in records:
    #             r["_areaId"] = i  # tag the source
    #         valid_ids.append(i)
    #         all_records.extend(records)
    #         save_to_csv(records, os.path.join("api_data", f"branchId_{i}.csv"))
    #     else:
    #         print("no data")
    #     time.sleep(0.2)  # polite delay between requests


    if all_records:
        # Combine all into one file
        keys = set()
        for r in all_records:
            keys.update(r.keys())
        headers = list(keys)
        combined_path = os.path.join("api_data", "subServiceId.csv")
        with open(combined_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for r in all_records:
                row = {k: normalize_value(r.get(k, "")) for k in headers}
                writer.writerow(row)
        print(f"\nCombined {len(all_records)} total records across {len(valid_ids)} valid IDs")
        print(f"Valid IDs with data: {valid_ids}")
    else:
        print("\nNo valid subServiceId found with data.")

if __name__ == "__main__":
    main()