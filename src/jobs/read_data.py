from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Replace with your folder
df = spark.read.parquet("/data/loaded_dataset_1766151176")

# Show first 10 rows
df.show(10)



