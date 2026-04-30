import pandas as pd
import numpy as np
import os
import sqlite3

# Loading cleaned Olympics data
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "data", "processed")

# Loading World Bank data
RAW_PATH = os.path.join(BASE_DIR, "data", "raw")

# Verify all input files exist before loading
required_files = {
    "olympics_cleaned": os.path.join(PROCESSED_PATH, "olympics_cleaned.csv"),
    "gdp": os.path.join(RAW_PATH, "gdp.csv"),
    "population": os.path.join(RAW_PATH, "population.csv"),
    "surface_area": os.path.join(RAW_PATH, "surface_area.csv"),
}
for name, path in required_files.items():
    if not os.path.exists(path):
        raise FileNotFoundError(f"Required file '{name}' not found at: {path}")

olympics = pd.read_csv(required_files["olympics_cleaned"])
gdp = pd.read_csv(required_files["gdp"])
population = pd.read_csv(required_files["population"])
area = pd.read_csv(required_files["surface_area"])
print(f"Loaded {len(olympics)} Olympic records.")

for df_wb in [gdp, population, area]:
    df_wb["year"] = pd.to_numeric(df_wb["year"], errors="coerce")
    df_wb.dropna(subset=["year"], inplace=True)
    df_wb["year"] = df_wb["year"].astype(int)

# NOC codes (Olympics) → ISO3 codes (World Bank)
# Historical nations (URS, GDR, TCH, YUG) map to their modern successors;
# World Bank data for those periods is sparse so rows may still be dropped later.
noc_to_iso = {
    "GER": "DEU",
    "SUI": "CHE",
    "DEN": "DNK",
    "GRE": "GRC",
    "NED": "NLD",
    "POR": "PRT",
    "RSA": "ZAF",
    "SLO": "SVN",
    "CRO": "HRV",
    "LAT": "LVA",
    "MGL": "MNG",
    "PHI": "PHL",
    "TRI": "TTO",
    "INA": "IDN",
    "MAS": "MYS",
    "SIN": "SGP",
    "TPE": "TWN",
    "VIE": "VNM",
    "ZIM": "ZWE",
    "ZAM": "ZMB",
    "IRI": "IRN",
    "NGR": "NGA",
    "CHI": "CHL",
    "URU": "URY",
    "PAR": "PRY",
    "BUL": "BGR",
    "GUA": "GTM",
    "HON": "HND",
    "ESA": "SLV",
    "CRC": "CRI",
    "NCA": "NIC",
    "DOM": "DOM",
    "JAM": "JAM",
    "BAH": "BHS",
    "PUR": "PRI",
    "HAI": "HTI",
    "FIJ": "FJI",
    "PNG": "PNG",
    "HKG": "HKG",
    "ISL": "ISL",
    "LUX": "LUX",
    "IRL": "IRL",
    "ALG": "DZA",
    "TUN": "TUN",
    "CIV": "CIV",
    "CMR": "CMR",
    "GHA": "GHA",
    "SEN": "SEN",
    "TAN": "TZA",
    "UGA": "UGA",
    "NIG": "NER",
    # Historical nations → modern successors (best effort)
    "URS": "RUS",   # Soviet Union → Russia
    "GDR": "DEU",   # East Germany → Germany
    "TCH": "CZE",   # Czechoslovakia → Czech Republic
    "YUG": "SRB",   # Yugoslavia → Serbia
    "SCG": "SRB",   # Serbia and Montenegro → Serbia
    "EUA": "DEU",   # United Team of Germany → Germany
    "BOH": "CZE",   # Bohemia → Czech Republic
}

# Creating iso_code column
olympics["iso_code"] = olympics["country_code"].map(noc_to_iso).fillna(olympics["country_code"])

# Removing unnecessary columns
gdp = gdp.drop(columns=["country"])
population = population.drop(columns=["country"])
area = area.drop(columns=["country"])

# Merging World Bank Datasets
wb = gdp.merge(population, on=["country_code", "year"], how="left")
wb = wb.merge(area, on=["country_code", "year"], how="left")

# Merging with Olympics Dataset
final_df = olympics.merge(
    wb,
    left_on=["iso_code", "year"],
    right_on=["country_code", "year"],
    how="left"
)

# Removing key rows with missing data
before = len(final_df)
final_df = final_df.dropna(subset=["gdp", "population"])
print(f"Dropped {before - len(final_df)} rows missing GDP or population data.")

# Removing impossible values
before = len(final_df)
final_df = final_df[(final_df["population"] > 0) & (final_df["gdp"] > 0)]
print(f"Dropped {before - len(final_df)} rows with non-positive GDP or population.")

before = len(final_df)
final_df = final_df[final_df["population"] > 100_000]
print(f"Dropped {before - len(final_df)} rows for tiny countries (population ≤ 100k).")

# Clean up duplicate country_code columns produced by the merge
if "country_code_y" in final_df.columns:
    final_df = final_df.drop(columns=["country_code_y"])
if "country_code_x" in final_df.columns:
    final_df = final_df.rename(columns={"country_code_x": "country_code"})

# Taking results after 1960 (due to data availability)
final_df = final_df[final_df["year"] >= 1960]

# Adding log features
final_df["log_gdp"] = np.log(final_df["gdp"])
final_df["log_population"] = np.log(final_df["population"])

# GDP per capita
final_df["gdp_per_capita"] = final_df["gdp"] / final_df["population"]
# Medals per million people
final_df["medals_per_million"] = final_df["medals_total"] / final_df["population"] * 1_000_000

# Creating a host dataset
host_data = [
    (1960, "ITA"),  # Rome
    (1964, "JPN"),  # Tokyo
    (1968, "MEX"),  # Mexico City
    (1972, "DEU"),  # Munich
    (1976, "CAN"),  # Montreal
    (1980, "RUS"),  # Moscow (USSR → RUS approximation)
    (1984, "USA"),  # Los Angeles
    (1988, "KOR"),  # Seoul
    (1992, "ESP"),  # Barcelona
    (1996, "USA"),  # Atlanta
    (2000, "AUS"),  # Sydney
    (2004, "GRC"),  # Athens
    (2008, "CHN"),  # Beijing
    (2012, "GBR"),  # London
    (2016, "BRA"),  # Rio
    (2020, "JPN"),  # Tokyo (held in 2021)
    (2024, "FRA"),  # Paris
]

host_df = pd.DataFrame(host_data, columns=["year", "iso_code"])

# Merging host data with our dataset
final_df = final_df.merge(
    host_df,
    on=["year", "iso_code"],
    how="left",
    indicator=True
)

final_df["host_flag"] = (final_df["_merge"] == "both").astype(int)
final_df = final_df.drop(columns=["_merge"])

print(f"Final dataset: {len(final_df)} rows, {len(final_df.columns)} columns.")

# Storing data in a relational database, queried using SQL

# Ensure folder exists
os.makedirs(PROCESSED_PATH, exist_ok=True)

conn = sqlite3.connect(os.path.join(PROCESSED_PATH, "olympics.db"))
try:
    final_df.to_sql("olympics_final", conn, if_exists="replace", index=False)
finally:
    conn.close()

#Saving the final dataset
final_df.to_csv(os.path.join(PROCESSED_PATH, "final_dataset.csv"), index=False)
print("Saved final_dataset.csv and olympics.db")
