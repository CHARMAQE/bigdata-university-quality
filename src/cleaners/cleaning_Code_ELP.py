# def clean_cod_elp(df):
#     """
#     Rule 1 – COD_ELP format normalization
#     - Uppercase
#     - Trim
#     - Invalid format -> null + flag
#     """

#     df = df.copy()

#     # Normalize first
#     df["COD_ELP"] = df["COD_ELP"].astype(str).str.strip().str.upper()

#     # Detect invalid format
#     df["elp_invalid_format"] = (
#         df["COD_ELP"].isna() |
#         (df["COD_ELP"].str.len() != 8) |
#         (~df["COD_ELP"].str.isalnum())
#     )

#     # Apply correction: invalidate bad codes
#     df.loc[df["elp_invalid_format"], "COD_ELP"] = None

#     # Metrics (optional log)
#     invalid_count = df["elp_invalid_format"].sum()
#     total = len(df)

#     print(
#         f"COD_ELP format check → invalid: {invalid_count} "
#         f"({invalid_count / total:.2%})"
#     )

#     return df


from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_cod_elp_spark(df: DataFrame) -> DataFrame:
    """
    Cleans COD_ELP anomalies in PySpark.
    Rule:
    - Uppercase + trim
    - Invalid format (not 8 alphanumeric chars) -> null + flag
    """
    
    # 1️⃣ Normalize: trim and uppercase
    df = df.withColumn("COD_ELP", F.upper(F.trim(F.col("COD_ELP"))))

    # 2️⃣ Flag invalid format
    df = df.withColumn(
        "elp_invalid_format",
        F.col("COD_ELP").isNull() |
        (F.length(F.col("COD_ELP")) != 8) |
        (~F.col("COD_ELP").rlike("^[a-zA-Z0-9]+$"))
    )

    # 3️⃣ Invalidate bad codes
    df = df.withColumn(
        "COD_ELP",
        F.when(F.col("elp_invalid_format"), None).otherwise(F.col("COD_ELP"))
    )

    # Optional: log metrics
    total = df.count()
    invalid_count = df.filter(F.col("elp_invalid_format")).count()
    print(f"COD_ELP format check → invalid: {invalid_count} ({invalid_count / total:.2%})")

    return df
