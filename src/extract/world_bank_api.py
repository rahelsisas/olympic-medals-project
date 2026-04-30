import requests
import pandas as pd
import os

BASE_URL = "https://api.worldbank.org/v2/country/all/indicator/{indicator}"


def fetch_world_bank_data(indicator, name):
    url = BASE_URL.format(indicator=indicator)
    params = {"format": "json", "per_page": 20000}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch {indicator} from World Bank API: {e}")

    if len(data) < 2 or data[1] is None:
        raise ValueError(f"Unexpected API response for indicator {indicator}")

    records = []
    for entry in data[1]:
        value = entry.get("value")
        if value is None:
            continue
        records.append({
            "country": entry.get("country", {}).get("value", ""),
            "country_code": entry.get("countryiso3code", ""),
            "year": entry.get("date", ""),
            name: value,
        })

    return pd.DataFrame(records)


if __name__ == "__main__":
    print("Fetching GDP...")
    gdp = fetch_world_bank_data("NY.GDP.MKTP.CD", "gdp")

    print("Fetching population...")
    population = fetch_world_bank_data("SP.POP.TOTL", "population")

    print("Fetching surface area...")
    area = fetch_world_bank_data("AG.SRF.TOTL.K2", "surface_area")

    # Save raw data
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
    os.makedirs(DATA_PATH, exist_ok=True)

    gdp.to_csv(os.path.join(DATA_PATH, "gdp.csv"), index=False)
    population.to_csv(os.path.join(DATA_PATH, "population.csv"), index=False)
    area.to_csv(os.path.join(DATA_PATH, "surface_area.csv"), index=False)
    print("World Bank data saved.")
