import pandas as pd
import os

# Load dataset
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

OUTPUT_PATH = os.path.join(BASE_DIR, "data", "processed")
FILE_PATH = os.path.join(BASE_DIR, "data", "raw", "olympics", "olympics_dataset.csv")

df = pd.read_csv(FILE_PATH)

# Ensure directory exists
os.makedirs(OUTPUT_PATH, exist_ok=True)

# Keep only Summer Olympics
df = df[df["Season"] == "Summer"]

# Remove rows without medals
df = df[df["Medal"] != "No medal"]

# Rename columns
df = df.rename(columns={
   "Year": "year",
   "Team": "country",
   "NOC": "country_code",
   "Medal": "medal"
})

# Convert medals into numeric columns
df["gold"] = (df["medal"] == "Gold").astype(int)
df["silver"] = (df["medal"] == "Silver").astype(int)
df["bronze"] = (df["medal"] == "Bronze").astype(int)

# Remove mixed teams (contain "/")
df = df[~df["country"].str.contains("/")]

# Remove weird team names
invalid_teams = [
   "Mixed team",
   "Refugee Olympic Team",
   "A North American Team"
]

df = df[~df["country"].isin(invalid_teams)]

# Keep only 3-letter country codes
df = df[df["country_code"].str.len() == 3]

# Aggregate to country-year level
agg_df = df.groupby(["year", "country_code"]).agg({
   "gold": "sum",
   "silver": "sum",
   "bronze": "sum"
}).reset_index()

agg_df["medals_total"] = (
   agg_df["gold"] + agg_df["silver"] + agg_df["bronze"]
)

# Map country names
noc_to_country = {
   "USA": "United States",
   "GBR": "United Kingdom",
   "GER": "Germany",
   "FRA": "France",
   "AUS": "Australia",
   "AUT": "Austria",
   "GRE": "Greece",
   "DEN": "Denmark",
   "HUN": "Hungary",
   "SUI": "Switzerland"
}

agg_df["country"] = agg_df["country_code"].map(noc_to_country)
agg_df["country"] = agg_df["country"].fillna(agg_df["country_code"])

agg_df = agg_df[[
   "year",
   "country_code",
   "country",
   "gold",
   "silver",
   "bronze",
   "medals_total"
]]

# Only taking data from 1960 onwards as it is more reliable
agg_df = agg_df[agg_df["year"] >= 1960]

# Save cleaned dataset
agg_df.to_csv(os.path.join(OUTPUT_PATH, "olympics_cleaned.csv"), index=False)