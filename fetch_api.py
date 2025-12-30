import requests
import csv
import os

# Create a folder to save CSV files
os.makedirs("api_data", exist_ok=True)

def fetch_json(url, key_name, filename):
    try:
        response = requests.get(url)
        print(f"\nFetching from {url}")
        if response.status_code == 200:
            data = response.json()

            # Check if main key exists
            if key_name in data:
                records = data[key_name]

                # Decide CSV headers based on data type
                if len(records) > 0:
                    first_item = records[0]
                    if "id" in first_item and "label" in first_item and "value" in first_item:
                        headers = ["id", "label", "value"]
                    elif "code" in first_item and "name" in first_item:
                        headers = ["code", "name"]
                    else:
                        headers = list(first_item.keys())
                else:
                    print(f"No data found in {filename}")
                    return

                # Save to CSV
                csv_path = os.path.join("api_data", f"{filename}.csv")
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    for item in records:
                        writer.writerow({k: item.get(k, "") for k in headers})

                print(f"Saved data to {csv_path}")

            else:
                print(f"Key '{key_name}' not found in response")

        else:
            print(f"Failed to fetch ({url}): {response.status_code}")

    except Exception as e:
        print(f"Error fetching/parsing {url}: {e}")

#API Fetch Functions

def fetch_analyte():
    print("\n==Analyte Data==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData/analyte", "referenceData", "analyte" )

def fetch_branch():
    print("\n==Branch DaTa==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData/branch", "referenceData", "branch")

def fetch_category():
    print("\n==Category Data==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData/category", "referenceData", "category")

def fetch_country():
    print("\n==Country Data==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData/country", "referenceData", "country")

def fetch_domain():
    print("\n==Domain Data==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData/domain", "domains", "domain")

def fetch_individualService():
    print("\n==Individual Service Data==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData/individualService", "referenceData", "individualService")

def fetch_metrologyArea():
    print("\n==Metrology Area Data==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData/metrologyArea", "referenceData", "MetrologyArea")

def fetch_nuclide():
    print("\n==Nuclide Data==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData", "referenceData", "Nuclide")

def fetch_quantity():
    print("\n==Quantity Data==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData", "referenceData", "quantity")

def fetch_radiationMedium():
    print("\n==Radiation Mediun Data==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData/radiationMedium", "referenceData", "radiationMedium")

def fetch_radiationSource():
    print("\n==Radiation Source Data==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData/radiationSource", "referenceData", "radiationSource")

def fetch_service():
    print("\n==Service Data==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData/service?branchId", "referenceData", "service")

def fetch_subService():
    print("\n==Subservice Data==")
    fetch_json("https://www.bipm.org/api/kcdb/referenceData/subService?serviceId", "referenceData", "subService")

# Main Execution

def main():
    print("Fetching JSON data from all 13 APIs\n")

    fetch_analyte()
    fetch_branch()
    fetch_category()
    fetch_country()
    fetch_domain()
    fetch_individualService()
    fetch_metrologyArea()
    fetch_nuclide()
    fetch_quantity()
    fetch_radiationMedium()
    fetch_radiationSource()
    fetch_service()
    fetch_subService()

    print("\nAll 13 API data fetched successfully!")


if __name__ == "__main__":
    main()
