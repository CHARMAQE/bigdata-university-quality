# def clean_cod_ses(df):
#     """
#     Clean COD_SES (session)
#     - Must be 1 or 2
#     - Nulls and invalids flagged
#     - row_status updated
#     """
#     df = df.copy()
    
#     # Convert to numeric (int)
#     df['COD_SES'] = pd.to_numeric(df['COD_SES'], errors='coerce')
    
#     # Detect missing
#     df['cod_ses_missing'] = df['COD_SES'].isna()
    
#     # Detect invalid values
#     df['cod_ses_invalid'] = ~df['COD_SES'].isin([1, 2])
    
#     # Invalidate incorrect values
#     df.loc[df['cod_ses_invalid'], 'COD_SES'] = None
    
#     # Update row_status
#     if 'row_status' not in df.columns:
#         df['row_status'] = 'OK'
    
#     df.loc[df['cod_ses_missing'] | df['cod_ses_invalid'], 'row_status'] = 'FLAGGED'
    
#     # Optional metrics
#     total = len(df)
#     print(f"COD_SES missing: {df['cod_ses_missing'].sum()} ({df['cod_ses_missing'].sum()/total:.2%})")
#     print(f"COD_SES invalid: {df['cod_ses_invalid'].sum()} ({df['cod_ses_invalid'].sum()/total:.2%})")
    
#     return df



from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_cod_ses_spark(df: DataFrame) -> DataFrame:
    """
    Clean COD_SES (session) using PySpark
    - Must be 1 or 2
    - Nulls and invalids flagged
    - row_status updated
    """

    # 1️⃣ Convert to numeric (int) — invalid → null
    df = df.withColumn(
        "COD_SES",
        F.col("COD_SES").cast("int")
    )

    # 2️⃣ Detect missing
    df = df.withColumn(
        "cod_ses_missing",
        F.col("COD_SES").isNull()
    )

    # 3️⃣ Detect invalid values
    df = df.withColumn(
        "cod_ses_invalid",
        ~F.col("COD_SES").isin(1, 2)
    )

    # 4️⃣ Invalidate incorrect values
    df = df.withColumn(
        "COD_SES",
        F.when(F.col("cod_ses_invalid"), None).otherwise(F.col("COD_SES"))
    )

    # 5️⃣ Ensure row_status exists
    if "row_status" not in df.columns:
        df = df.withColumn("row_status", F.lit("OK"))

    # 6️⃣ Update row_status
    df = df.withColumn(
        "row_status",
        F.when(
            F.col("cod_ses_missing") | F.col("cod_ses_invalid"),
            F.lit("FLAGGED")
        ).otherwise(F.col("row_status"))
    )

    # Optional metrics (for logs / report)
    total = df.count()

    missing_count = df.filter(F.col("cod_ses_missing")).count()
    invalid_count = df.filter(F.col("cod_ses_invalid")).count()


    return df

