from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("Olympics").getOrCreate()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
FILE_PATH = os.path.join(BASE_DIR, "data", "processed", "final_dataset.csv")

df = spark.read.csv(FILE_PATH, header=True, inferSchema=True)

df.groupBy("year").sum("medals_total").show()