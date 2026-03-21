# Olympic Medals Data Engineering Project

## Research Question
Do wealthier countries (GDP), larger countries (population / size), or host countries win more Olympic medals?

---

## Data Sources
- Olympic medals dataset (Kaggle)
- World Bank API:
  - GDP (NY.GDP.MKTP.CD)
  - Population (SP.POP.TOTL)
  - Surface area (AG.SRF.TOTL.K2)

---

## Project Structure
```
olympic-medals-project/
│
├── data/
│ ├── raw/ # Raw datasets
│ └── processed/ # Cleaned & final datasets
│
├── src/
│ ├── extract/ # Data ingestion (World Bank API)
│ ├── transform/ # Cleaning, merging, feature engineering
│ └── run_pipeline.py # (optional) pipeline runner
│
├── sql/ # SQL queries
├── docs/ # Documentation & explanations
├── notebooks/ # Exploration
│
└── README.md
```

---

## Data Pipeline

The project follows a standard ETL pipeline:

### 1. Extract
- Olympic data loaded from Kaggle
- World Bank data fetched via API

### 2. Transform
- Clean Olympic dataset (remove non-medal entries, aggregate to country-year)
- Handle messy team names using NOC codes
- Convert NOC → ISO country codes
- Merge with World Bank data

### 3. Load
- Final dataset saved as CSV
- Stored in SQLite database

---

## Key Data Engineering Challenges

### 1. Country Code Mismatch
- Olympic data uses NOC codes (e.g. GER, SUI)
- World Bank uses ISO3 codes (DEU, CHE)
- Solution: mapping layer (NOC → ISO)

---

### 2. Missing Data (Pre-1960)
- World Bank data unavailable before ~1960
- Solution: restrict dataset to 1960+

---

### 3. Messy Team Names
- Dataset contained clubs, mixed teams, historical entities
- Solution: rely on structured country codes instead of names

---

### 4. Multi-source Integration
- Combined 4 datasets into one unified dataset
- Ensured consistent schema and keys

---

## Features Created

- Total medals
- GDP per capita
- Medals per million population
- Log GDP and log population
- Host country indicator

---

## Technologies Used

- Python (pandas, requests)
- PySpark
- SQLite
- Git & GitHub

---

## How to Run

1. Install dependencies:
```python
pip install -r requirements.txt
```

