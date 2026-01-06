from pyspark.sql import SparkSession
from cleaners.clean_cod_anu import clean_cod_anu_spark
from cleaners.clean_cod_etud import clean_cod_etu_spark
from cleaners.clean_NOT_ELP import clean_not_elp_spark
from cleaners.cleaning_Code_ELP import clean_cod_elp_spark
from cleaners.cleaning_COD_TRE import clean_cod_tre_spark
from cleaners.cleaning_cod_ses import clean_cod_ses_spark

spark = SparkSession.builder \
    .appName("Cleaning_data_pipline") \
    .getOrCreate()

df = spark.read.parquet("/data/loaded_dataset/output")

df_clean = clean_cod_anu_spark(df)

df_clean = clean_cod_etu_spark(df_clean)

df_clean = clean_cod_elp_spark(df_clean)

df_clean = clean_not_elp_spark(df_clean)

df_clean = clean_cod_tre_spark(df_clean)

df_clean = clean_cod_ses_spark(df_clean)



df_clean.show(30, truncate=False)


# Write CSV
df_clean.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/data/cleaned_data/output")

spark.stop()
