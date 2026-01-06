from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Preview_CSV") \
    .getOrCreate()

# Read the CSV
df = spark.read.option("header", "true").csv("/data/cleaned_dataset")

# Show first 20 rows (default)
df.show()

# Show first 30 rows without truncating columns
df.show(30, truncate=False)

spark.stop()