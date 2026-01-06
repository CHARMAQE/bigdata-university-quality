# def clean_cod_etu(df):
#     """
#     Cleans COD_ETU anomalies based on business rules.
#     Business key = (COD_ETU, COD_SES, COD_ELP)
#     """

#     df = df.copy()

#     # ===============================
#     # 1️⃣ Detection — Missing values
#     # ===============================
#     df["etu_missing"] = df["COD_ETU"].isna()

#     null_count = df["etu_missing"].sum()
#     null_rate = df["etu_missing"].mean()

#     # ===============================
#     # 2️⃣ Detection — Business duplicates
#     # ===============================
#     business_key = ["COD_ETU", "COD_SES", "COD_ELP"]

#     df["dup_business_flag"] = df.duplicated(
#         subset=business_key,
#         keep="first"
#     )

#     num_business_duplicates = df["dup_business_flag"].sum()

#     # ===============================
#     # 3️⃣ Optional — Session-level duplicates (analysis only)
#     # ===============================
#     df["dup_session_flag"] = df.duplicated(
#         subset=["COD_ETU", "COD_SES"],
#         keep=False
#     )

#     # ===============================
#     # 4️⃣ Correction — Remove true duplicates
#     # ===============================
#     df_clean = df[~df["dup_business_flag"]].copy()

#     # ===============================
#     # 5️⃣ Metrics (for logging / report)
#     # ===============================
#     metrics = {
#         "nulls_cod_etu": int(null_count),
#         "null_rate_cod_etu": float(null_rate),
#         "business_duplicates_removed": int(num_business_duplicates),
#         "rows_before": len(df),
#         "rows_after": len(df_clean)
#     }

#     print("COD_ETU cleaning metrics:", metrics)

#     return df_clean


from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window



def clean_cod_etu_spark(df: DataFrame) -> DataFrame:
    """
    Cleans COD_ETU anomalies using PySpark.
    Business key = (COD_ETU, COD_SES, COD_ELP)
    Produces same metrics as the pandas version.
    """

    # ===============================
    # 1️⃣ Detection — Missing values
    # ===============================
    df = df.withColumn("etu_missing", F.col("COD_ETU").isNull())

    total_rows = df.count()
    null_count = df.filter(F.col("etu_missing")).count()
    null_rate = null_count / total_rows if total_rows > 0 else 0.0

    # ===============================
    # 2️⃣ Detection — Business duplicates
    # ===============================
    business_key = ["COD_ETU", "COD_SES", "COD_ELP"]

    window_business = Window.partitionBy(*business_key).orderBy(F.lit(1))
    df = df.withColumn("row_number", F.row_number().over(window_business))
    df = df.withColumn("dup_business_flag", F.col("row_number") > 1)

    num_business_duplicates = df.filter(F.col("dup_business_flag")).count()

    # ===============================
    # 3️⃣ Optional — Session-level duplicates (analysis only)
    # ===============================
    window_session = Window.partitionBy("COD_ETU", "COD_SES")
    df = df.withColumn("dup_session_flag", F.count("*").over(window_session) > 1)

    # ===============================
    # 4️⃣ Correction — Remove true duplicates
    # ===============================
    df_clean = df.filter(~F.col("dup_business_flag"))

    # ===============================
    # 5️⃣ Metrics (for logging / report)
    # ===============================
    metrics = {
        "nulls_cod_etu": null_count,
        "null_rate_cod_etu": float(null_rate),
        "business_duplicates_removed": num_business_duplicates,
        "rows_before": total_rows,
        "rows_after": df_clean.count()
    }

    print("COD_ETU cleaning metrics:", metrics)

    # Drop helper columns if needed
    df_clean = df_clean.drop("etu_missing", "row_number", "dup_business_flag", "dup_session_flag")

    return df_clean