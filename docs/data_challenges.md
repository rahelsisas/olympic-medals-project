# Data Engineering Challenges & Solutions

## 1. Country Code Inconsistency

Problem:
Olympic data uses NOC codes (e.g. GER, SUI), while World Bank uses ISO3 codes (DEU, CHE).

Solution:
A mapping layer was created to convert NOC → ISO3 before merging datasets.

---

## 2. Missing Economic Data

Problem:
World Bank data is not available for early Olympic years (before ~1960).

Solution:
The dataset was filtered to include only years >= 1960 to ensure completeness.

---

## 3. Messy Olympic Data

Problem:
The Olympic dataset contains:
- club teams
- mixed teams
- historical entities

Solution:
Instead of cleaning names manually, we used structured identifiers (country codes) to ensure consistency.

---

## 4. Multi-source Integration

Problem:
Data came from multiple sources with different schemas.

Solution:
- Standardized column names
- Unified keys (country_code + year)
- Performed left joins to preserve Olympic records

---

## 5. Environment Setup Challenges

Problem:
PySpark required specific versions of Python and Java.

Solution:
- Python 3.11 environment
- Compatible PySpark version
- Installed JDK and configured JAVA_HOME