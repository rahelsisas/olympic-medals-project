import pandas as pd
import os

# Load dataset
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "processed")
FILE_PATH = os.path.join(BASE_DIR, "data", "raw", "olympics", "olympics_dataset.csv")

if not os.path.exists(FILE_PATH):
    raise FileNotFoundError(f"Olympics dataset not found at: {FILE_PATH}")

df = pd.read_csv(FILE_PATH)
print(f"Loaded {len(df)} rows from raw Olympics dataset.")

# Ensure directory exists
os.makedirs(OUTPUT_PATH, exist_ok=True)

# Keep only Summer Olympics
df = df[df["Season"] == "Summer"]
print(f"Rows after keeping Summer Olympics: {len(df)}")

# Remove rows without medals
df = df[df["Medal"] != "No medal"]
print(f"Rows after removing non-medal rows: {len(df)}")

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
df = df[~df["country"].str.contains("/", regex=False)]
print(f"Rows after removing mixed teams: {len(df)}")

# Remove weird team names
invalid_teams = ["Mixed team", "Refugee Olympic Team", "A North American Team"]
df = df[~df["country"].isin(invalid_teams)]
print(f"Rows after removing invalid teams: {len(df)}")

# Keep only 3-letter country codes
df = df[df["country_code"].str.len() == 3]
print(f"Rows after keeping 3-letter codes: {len(df)}")

# Aggregate to country-year level
agg_df = df.groupby(["year", "country_code"]).agg({
    "gold": "sum",
    "silver": "sum",
    "bronze": "sum"
}).reset_index()

agg_df["medals_total"] = agg_df["gold"] + agg_df["silver"] + agg_df["bronze"]

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
    "SUI": "Switzerland",
    "CHN": "China",
    "RUS": "Russia",
    "URS": "Soviet Union",
    "GDR": "East Germany",
    "ITA": "Italy",
    "CAN": "Canada",
    "ESP": "Spain",
    "JPN": "Japan",
    "KOR": "South Korea",
    "NED": "Netherlands",
    "NOR": "Norway",
    "SWE": "Sweden",
    "FIN": "Finland",
    "BEL": "Belgium",
    "BRA": "Brazil",
    "MEX": "Mexico",
    "ARG": "Argentina",
    "CUB": "Cuba",
    "POL": "Poland",
    "ROU": "Romania",
    "BUL": "Bulgaria",
    "TUR": "Turkey",
    "IRI": "Iran",
    "KAZ": "Kazakhstan",
    "UKR": "Ukraine",
    "NZL": "New Zealand",
    "NGR": "Nigeria",
    "ETH": "Ethiopia",
    "KEN": "Kenya",
    "EGY": "Egypt",
    "MAR": "Morocco",
    "COL": "Colombia",
    "VEN": "Venezuela",
    "CHI": "Chile",
    "PER": "Peru",
    "ECU": "Ecuador",
    "URU": "Uruguay",
    "PAR": "Paraguay",
    "RSA": "South Africa",
    "SLO": "Slovenia",
    "CRO": "Croatia",
    "LAT": "Latvia",
    "LTU": "Lithuania",
    "EST": "Estonia",
    "MGL": "Mongolia",
    "PHI": "Philippines",
    "TRI": "Trinidad and Tobago",
    "INA": "Indonesia",
    "MAS": "Malaysia",
    "SIN": "Singapore",
    "TPE": "Chinese Taipei",
    "VIE": "Vietnam",
    "ZIM": "Zimbabwe",
    "ZAM": "Zambia",
    "TCH": "Czechoslovakia",
    "YUG": "Yugoslavia",
    "SCG": "Serbia and Montenegro",
    "EUA": "United Team of Germany",
    "BOH": "Bohemia",
    "POR": "Portugal",
    "CZE": "Czech Republic",
    "SVK": "Slovakia",
    "SRB": "Serbia",
    "SVN": "Slovenia",
    "HRV": "Croatia",
    "BLR": "Belarus",
    "AZE": "Azerbaijan",
    "GEO": "Georgia",
    "ARM": "Armenia",
    "UZB": "Uzbekistan",
    "TJK": "Tajikistan",
    "TKM": "Turkmenistan",
    "KGZ": "Kyrgyzstan",
    "MDA": "Moldova",
    "IND": "India",
    "PAK": "Pakistan",
    "THA": "Thailand",
    "PRK": "North Korea",
    "IRQ": "Iraq",
    "SYR": "Syria",
    "LBN": "Lebanon",
    "ISR": "Israel",
    "SAU": "Saudi Arabia",
    "QAT": "Qatar",
    "UAE": "United Arab Emirates",
    "BHR": "Bahrain",
    "KWT": "Kuwait",
    "JOR": "Jordan",
    "CIV": "Ivory Coast",
    "CMR": "Cameroon",
    "GHA": "Ghana",
    "SEN": "Senegal",
    "TAN": "Tanzania",
    "UGA": "Uganda",
    "ALG": "Algeria",
    "TUN": "Tunisia",
    "LBA": "Libya",
    "NIG": "Niger",
    "MLI": "Mali",
    "BUR": "Burkina Faso",
    "NCA": "Nicaragua",
    "CRC": "Costa Rica",
    "PAN": "Panama",
    "BOL": "Bolivia",
    "GUA": "Guatemala",
    "HON": "Honduras",
    "ESA": "El Salvador",
    "DOM": "Dominican Republic",
    "JAM": "Jamaica",
    "TTO": "Trinidad and Tobago",
    "BAH": "Bahamas",
    "PUR": "Puerto Rico",
    "HAI": "Haiti",
    "POC": "Polynesia",
    "FIJ": "Fiji",
    "PNG": "Papua New Guinea",
    "HKG": "Hong Kong",
    "MAC": "Macau",
    "MON": "Monaco",
    "LUX": "Luxembourg",
    "IRL": "Ireland",
    "ISL": "Iceland",
}

agg_df["country"] = agg_df["country_code"].map(noc_to_country)
agg_df["country"] = agg_df["country"].fillna(agg_df["country_code"])

agg_df = agg_df[["year", "country_code", "country", "gold", "silver", "bronze", "medals_total"]]

# Only taking data from 1960 onwards as it is more reliable
agg_df = agg_df[agg_df["year"] >= 1960]
print(f"Final rows after 1960 filter: {len(agg_df)}")

# Save cleaned dataset
agg_df.to_csv(os.path.join(OUTPUT_PATH, "olympics_cleaned.csv"), index=False)
print("Saved olympics_cleaned.csv")
