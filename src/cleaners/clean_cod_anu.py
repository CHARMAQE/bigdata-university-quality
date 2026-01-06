# import pandas as pd
# import numpy as np
# import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# def clean_cod_anu(df):
#     """
#     Clean and normalize COD_ANU column
#     Pipeline version:
#     - Internal detection & metrics
#     - Output only business columns
#     """

#     df = df.copy()

#     # =========================
#     # 1. Detection
#     # =========================
#     def detect_cod_anu_format(x):
#         if pd.isna(x):
#             return "NULL"
#         x = str(x)
#         if re.match(r"^\d{4}-\d{4}$", x):
#             return "YYYY-YYYY"
#         elif re.match(r"^\d{2}/\d{2}$", x):
#             return "YY/YY"
#         elif re.match(r"^\d{4}$", x):
#             return "YYYY"
#         else:
#             return "INVALID"

#     format_before = df["COD_ANU"].apply(detect_cod_anu_format)

#     # =========================
#     # 2. Correction / Normalization
#     # =========================
#     def normalize_cod_anu(x):
#         if pd.isna(x):
#             return np.nan

#         x = str(x)

#         if re.match(r"^\d{4}-\d{4}$", x):
#             return x

#         if re.match(r"^\d{4}$", x):
#             year = int(x)
#             return f"{year}-{year+1}"

#         if re.match(r"^\d{2}/\d{2}$", x):
#             start = int(x[:2]) + 2000
#             return f"{start}-{start+1}"

#         return np.nan

#     # Flag corrections (before normalization)
#     df["cod_anu_was_corrected"] = format_before != "YYYY-YYYY"

#     # Apply normalization
#     df["COD_ANU"] = df["COD_ANU"].apply(normalize_cod_anu)

#     # =========================
#     # 3. Optional metrics (kept internally)
#     # =========================
#     # These are computed but not returned
#     null_rate = df["COD_ANU"].isna().mean()
#     invalid_rate = (df["COD_ANU"].apply(detect_cod_anu_format) != "YYYY-YYYY").mean()
#     corrected_rows = df["cod_anu_was_corrected"].sum()

#     # (You could log or store these elsewhere if needed)

#     # =========================
#     # 4. Final pipeline output
#     # =========================
#     df_final = df[[
#         "COD_ANU",
#         "COD_ETU",
#         "COD_ELP",
#         "NOT_ELP",
#         "COD_TRE",
#         "COD_SES",
#         "row_status"
#     ]]

#     return df_final


def clean_cod_anu_spark(df: DataFrame) -> DataFrame:
    """
    Clean and normalize COD_ANU column using PySpark
    Pipeline version:
    - Format detection
    - Normalization
    - Correction flag
    - Internal DQ metrics (not returned)
    """

    # =========================
    # 1. Detection
    # =========================
    df = df.withColumn(
        "cod_anu_format",
        F.when(F.col("COD_ANU").isNull(), "NULL")
         .when(F.col("COD_ANU").rlike(r"^\d{4}-\d{4}$"), "YYYY-YYYY")
         .when(F.col("COD_ANU").rlike(r"^\d{2}/\d{2}$"), "YY/YY")
         .when(F.col("COD_ANU").rlike(r"^\d{4}$"), "YYYY")
         .otherwise("INVALID")
    )

    # =========================
    # 2. Correction flag
    # =========================
    df = df.withColumn(
        "cod_anu_was_corrected",
        F.col("cod_anu_format") != "YYYY-YYYY"
    )

    # =========================
    # 3. Normalization
    # =========================
    df = df.withColumn(
        "COD_ANU",
        F.when(F.col("COD_ANU").rlike(r"^\d{4}-\d{4}$"),
               F.col("COD_ANU"))

         .when(F.col("COD_ANU").rlike(r"^\d{4}$"),
               F.concat(
                   F.col("COD_ANU"),
                   F.lit("-"),
                   (F.col("COD_ANU").cast("int") + 1).cast("string")
               ))

         .when(F.col("COD_ANU").rlike(r"^\d{2}/\d{2}$"),
               F.concat(
                   (F.substring(F.col("COD_ANU"), 1, 2).cast("int") + 2000).cast("string"),
                   F.lit("-"),
                   (F.substring(F.col("COD_ANU"), 4, 2).cast("int") + 2000).cast("string")  # FIX: use positions 4-5, not 1-2 again
               ))

         .otherwise(F.lit(None))
    )

    # =========================
    # 4. Internal DQ metrics (not returned)
    # =========================
    total_rows = df.count()

    null_rate = (
        df.filter(F.col("COD_ANU").isNull()).count() / total_rows
        if total_rows > 0 else 0
    )

    invalid_rate = (
        df.filter(~F.col("COD_ANU").rlike(r"^\d{4}-\d{4}$")).count() / total_rows
        if total_rows > 0 else 0
    )

    corrected_rows = df.filter(F.col("cod_anu_was_corrected")).count()

    # (log/store metrics if needed)
    # print(null_rate, invalid_rate, corrected_rows)

    # =========================
    # 5. Final pipeline output
    # =========================
    df_final = df.select(
        "COD_ANU",
        "COD_ETU",
        "COD_ELP",
        "NOT_ELP",
        "COD_TRE",
        "COD_SES",
        "row_status"
    )

    return df_final
