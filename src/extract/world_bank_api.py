import requests
import pandas as pd
import os

BASE_URL = "https://api.worldbank.org/v2/country/all/indicator/{indicator}"

def fetch_world_bank_data(indicator, name):
    url = BASE_URL.format(indicator=indicator)
    
    params = {
        "format": "json",
        "per_page": 20000
    }

    response = requests.get(url, params=params)
    data = response.json()

    records = []

    for entry in data[1]:
        records.append({
            "country": entry["country"]["value"],
            "country_code": entry["countryiso3code"],
            "year": entry["date"],
            name: entry["value"]
        })

    df = pd.DataFrame(records)
    return df


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

    gdp.to_csv(os.path.join(DATA_PATH, "gdp.csv"), index=False)
    population.to_csv(os.path.join(DATA_PATH, "population.csv"), index=False)
    area.to_csv(os.path.join(DATA_PATH, "surface_area.csv"), index=False)