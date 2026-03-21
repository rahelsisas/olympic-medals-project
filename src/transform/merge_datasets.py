import pandas as pd
import numpy as np
import os
import sqlite3

# Loading cleaned Olympics data
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "data", "processed")

olympics = pd.read_csv(os.path.join(PROCESSED_PATH, "olympics_cleaned.csv"))

# Loading World Bank data
RAW_PATH = os.path.join(BASE_DIR, "data", "raw")

gdp = pd.read_csv(os.path.join(RAW_PATH, "gdp.csv"))
population = pd.read_csv(os.path.join(RAW_PATH, "population.csv"))
area = pd.read_csv(os.path.join(RAW_PATH, "surface_area.csv"))

# Converting year to int
gdp["year"] = gdp["year"].astype(int)
population["year"] = population["year"].astype(int)
area["year"] = area["year"].astype(int)

# Renaming for consistency
gdp = gdp.rename(columns={"country_code": "country_code"})
population = population.rename(columns={"country_code": "country_code"})
area = area.rename(columns={"country_code": "country_code"})

# Matching the different country codes of the two datasets
noc_to_iso = {
    "GER": "DEU",
    "SUI": "CHE",
    "DEN": "DNK",
    "GRE": "GRC",
    "AUT": "AUT",
    "FRA": "FRA",
    "USA": "USA",
    "GBR": "GBR",
    "ESP": "ESP",
    "ITA": "ITA",
    "CAN": "CAN",
    "AUS": "AUS",
    "BEL": "BEL",
    "NED": "NLD",
    "NOR": "NOR",
    "SWE": "SWE",
    "FIN": "FIN",
    "JPN": "JPN",
    "CHN": "CHN",
    "BRA": "BRA"
}

# Creating iso_code column
olympics["iso_code"] = olympics["country_code"].map(noc_to_iso)

# Fallback for already matching ones
olympics["iso_code"] = olympics["iso_code"].fillna(olympics["country_code"])

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
final_df = final_df.dropna(subset=["gdp", "population"])

# Removing impossible values
final_df = final_df[final_df["population"] > 0]
final_df = final_df[final_df["gdp"] > 0]

# Removing tiny countries 
final_df = final_df[final_df["population"] > 100_000]

# Dropping duplicate columns
final_df = final_df.drop(columns=["country_code_y"])
final_df = final_df.rename(columns={"country_code_x": "country_code"})

# Taking results after 1960 (due to data availability)
final_df = final_df[final_df["year"] >= 1960]

# Adding log features
final_df["log_gdp"] = np.log(final_df["gdp"])
final_df["log_population"] = np.log(final_df["population"])

# GDP per capita
final_df["gdp_per_capita"] = final_df["gdp"] / final_df["population"]

# Medals per million people
final_df["medals_per_million"] = (
    final_df["medals_total"] / final_df["population"] * 1_000_000
)

# Creating a host dataset
host_data = [
    (1960, "ITA"),  # Rome
    (1964, "JPN"),  # Tokyo
    (1968, "MEX"),  # Mexico City
    (1972, "DEU"),  # Munich
    (1976, "CAN"),  # Montreal
    (1980, "RUS"),  # Moscow (USSR → RUS approx)
    (1984, "USA"),  # Los Angeles
    (1988, "KOR"),  # Seoul
    (1992, "ESP"),  # Barcelona
    (1996, "USA"),  # Atlanta
    (2000, "AUS"),  # Sydney
    (2004, "GRC"),  # Athens
    (2008, "CHN"),  # Beijing
    (2012, "GBR"),  # London
    (2016, "BRA"),  # Rio
    (2020, "JPN")   # Tokyo (held in 2021)
]

host_df = pd.DataFrame(host_data, columns=["year", "iso_code"])

# Merging host data with our dataset
final_df = final_df.merge(
    host_df,
    left_on=["year", "iso_code"],
    right_on=["year", "iso_code"],
    how="left",
    indicator=True
)

final_df["host_flag"] = (final_df["_merge"] == "both").astype(int)
final_df = final_df.drop(columns=["_merge"])

# Storing data in a relational database, queried using SQL
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "data", "processed")

# Ensure folder exists
os.makedirs(PROCESSED_PATH, exist_ok=True)

conn = sqlite3.connect(os.path.join(PROCESSED_PATH, "olympics.db"))
final_df.to_sql("olympics_final", conn, if_exists="replace", index=False)
conn.close()

#Saving the final dataset
final_df.to_csv(os.path.join(PROCESSED_PATH, "final_dataset.csv"), index=False)