from pyspark.sql import SparkSession
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
FILE_PATH = os.path.join(BASE_DIR, "data", "processed", "final_dataset.csv")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "processed", "medals_by_year.csv")

if not os.path.exists(FILE_PATH):
    raise FileNotFoundError(f"final_dataset.csv not found at: {FILE_PATH}")

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

spark = SparkSession.builder.appName("OlympicMedals").getOrCreate()

try:
    df = spark.read.csv(FILE_PATH, header=True, inferSchema=True)

    medals_by_year = (
        df.groupBy("year")
        .sum("medals_total")
        .withColumnRenamed("sum(medals_total)", "total_medals")
        .orderBy("year")
    )
    medals_by_year.show()
    medals_by_year.toPandas().to_csv(OUTPUT_PATH, index=False)
    print(f"Saved medals_by_year.csv with {medals_by_year.count()} rows.")
finally:
    spark.stop()
