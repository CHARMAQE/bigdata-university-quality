from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import shutil
import os
import time

output_path = "/data/loaded_dataset/output"

spark = SparkSession.builder \
    .appName("Data_Ingestion") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

EXPECTED_COLS = 6

raw = spark.read.text("/data/raw/dataset_metier.txt").withColumnRenamed("value", "raw_line")

df_split = raw.withColumn("parts", F.split(F.col("raw_line"), ","))

df_parsed = df_split.withColumn(
    "parsed",
    F.when(
        F.size("parts") == EXPECTED_COLS,
        F.concat("parts", F.array(F.lit("OK")))
    )
    .when(
        F.size("parts") == EXPECTED_COLS + 1,
        F.array(
            F.col("parts")[0],
            F.col("parts")[1],
            F.col("parts")[2],
            F.concat_ws(",", F.col("parts")[3], F.col("parts")[4]),
            F.col("parts")[5],
            F.col("parts")[6],
            F.lit("FIXED")
        )
    )
    .otherwise(
        F.array(
            F.lit(None), F.lit(None), F.lit(None),
            F.lit(None), F.lit(None), F.lit(None),
            F.lit("INVALID")
        )
    )
)

df = df_parsed.select(
    F.col("parsed")[0].alias("COD_ANU"),
    F.col("parsed")[1].alias("COD_ETU"),
    F.col("parsed")[2].alias("COD_ELP"),
    F.col("parsed")[3].alias("NOT_ELP"),
    F.col("parsed")[4].alias("COD_TRE"),
    F.col("parsed")[5].alias("COD_SES"),
    F.col("parsed")[6].alias("row_status")
)

# Remove header row
df = df.filter(F.col("COD_ANU") != "COD_ANU")

# Clean output directory
if os.path.exists(output_path):
    shutil.rmtree(output_path)

# Write with legacy format for pyarrow compatibility
df.write.mode("overwrite").parquet(output_path)

print(f"Saved {df.count()} rows to {output_path}")
df.show(5)

spark.stop()