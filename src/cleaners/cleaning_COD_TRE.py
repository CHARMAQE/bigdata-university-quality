# def clean_cod_tre(df):
#     """
#     Clean COD_TRE (Résultat / Statut académique)
#     - Uppercase + strip
#     - Flag invalid codes
#     - Update row_status
#     """
#     df = df.copy()

#     # Allowed codes
#     valid_codes = {"V", "RAT", "VAR", "ADM", "NV", "ACAR" , "ABSN"}

#     # Normalize
#     df["COD_TRE"] = df["COD_TRE"].astype(str).str.strip().str.upper()

#     # Detect invalid codes
#     df["cod_tre_invalid"] = ~df["COD_TRE"].isin(valid_codes) | df["COD_TRE"].isna()

#     # Invalidate incorrect codes
#     df.loc[df["cod_tre_invalid"], "COD_TRE"] = None

#     # Update row_status
#     if "row_status" not in df.columns:
#         df["row_status"] = "OK"

#     df.loc[df["cod_tre_invalid"], "row_status"] = "FLAGGED"

#     # Optional metrics
#     invalid_count = df["cod_tre_invalid"].sum()
#     total_count = len(df)
#     print(f"COD_TRE invalid codes: {invalid_count} ({invalid_count / total_count:.2%})")

#     return df


from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_cod_tre_spark(df: DataFrame) -> DataFrame:
    """
    Clean COD_TRE (Résultat / Statut académique) using PySpark
    - Uppercase + trim
    - Flag invalid codes
    - Invalidate incorrect values
    - Update row_status
    """

    valid_codes = ["V","AJ","RAT", "VAR", "ADM", "NV", "ACAR", "ABSN"]

    # 1️⃣ Normalize
    df = df.withColumn(
        "COD_TRE",
        F.upper(F.trim(F.col("COD_TRE")))
    )

    # 2️⃣ Detect invalid codes (but allow NULL - student may not have result yet)
    df = df.withColumn(
        "cod_tre_invalid",
        ~(F.col("COD_TRE").isin(valid_codes) | F.col("COD_TRE").isNull())  # FIX: Allow nulls
    )

    # 3️⃣ Invalidate incorrect codes
    df = df.withColumn(
        "COD_TRE",
        F.when(F.col("cod_tre_invalid"), None).otherwise(F.col("COD_TRE"))
    )

    # 4️⃣ Ensure row_status exists
    if "row_status" not in df.columns:
        df = df.withColumn("row_status", F.lit("OK"))

    # 5️⃣ Update row_status
    df = df.withColumn(
        "row_status",
        F.when(F.col("cod_tre_invalid"), F.lit("FLAGGED"))
         .otherwise(F.col("row_status"))
    )

    # Optional metrics
    total = df.count()
    invalid_count = df.filter(F.col("cod_tre_invalid")).count()

    print(
        f"COD_TRE invalid codes: {invalid_count} "
        f"({invalid_count / total:.2%})"
    )

    return df
