from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("Olympics").getOrCreate()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
FILE_PATH = os.path.join(BASE_DIR, "data", "processed", "final_dataset.csv")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "processed", "medals_by_year.csv")

df = spark.read.csv(FILE_PATH, header=True, inferSchema=True)

medals_by_year = df.groupBy("year").sum("medals_total").orderBy("year")
medals_by_year.show()
medals_by_year.toPandas().to_csv(OUTPUT_PATH, index=False)

spark.stop()