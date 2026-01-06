# def clean_not_elp(df):
#     df = df.copy()
    
#     # 1️⃣ Convert to numeric
#     df['NOT_ELP'] = pd.to_numeric(df['NOT_ELP'], errors='coerce')
    
#     # 2️⃣ Flags
#     df['note_missing_s1'] = df['NOT_ELP'].isna() & (df['COD_SES']==1)
#     df['note_missing_s2'] = df['NOT_ELP'].isna() & (df['COD_SES']==2)
    
#     df['note_out_of_range'] = (df['NOT_ELP'] < 0) | (df['NOT_ELP'] > 20)
    
#     df['note_incoherent'] = (
#         ((df['COD_TRE']=='V') & (df['NOT_ELP'] < 10)) |
#         ((df['COD_TRE']=='R') & (df['NOT_ELP'] >= 10))
#     )
    
#     # 3️⃣ Duplicates per student/module/session
#     df['dup_note_flag'] = df.duplicated(subset=['COD_ETU','COD_ELP','COD_SES'], keep='first')
    
#     # Remove duplicates if needed
#     df = df[~df['dup_note_flag']].copy()
    
#     return df


from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

def clean_not_elp_spark(df: DataFrame) -> DataFrame:

    # 1️⃣ Normalize NOT_ELP
    df = df.withColumn("NOT_ELP", F.trim(F.col("NOT_ELP")))

    df = df.withColumn(
        "NOT_ELP",
        F.when(
            (F.col("NOT_ELP") == "") |
            (~F.col("NOT_ELP").rlike("^[0-9]+([.,][0-9]+)?$")),
            None
        ).otherwise(
            F.regexp_replace(F.col("NOT_ELP"), ",", ".")
        )
    )

    # 2️⃣ Safe cast
    df = df.withColumn("NOT_ELP", F.col("NOT_ELP").cast("double"))

    # 3️⃣ Flags
    df = df.withColumn("note_missing_s1", (F.col("NOT_ELP").isNull()) & (F.col("COD_SES") == 1))
    df = df.withColumn("note_missing_s2", (F.col("NOT_ELP").isNull()) & (F.col("COD_SES") == 2))
    df = df.withColumn("note_out_of_range", (F.col("NOT_ELP") < 0) | (F.col("NOT_ELP") > 20))
    df = df.withColumn(
        "note_incoherent",
        ((F.col("COD_TRE") == "V") & (F.col("NOT_ELP") < 10)) |
        ((F.col("COD_TRE") == "AJ") & (F.col("NOT_ELP") >= 10))  # FIX: AJ means failed, should be <10
    )

    # 4️⃣ Deduplication
    window = Window.partitionBy("COD_ETU", "COD_ELP", "COD_SES").orderBy(F.lit(1))
    df = df.withColumn("row_number", F.row_number().over(window))
    df_clean = df.filter(F.col("row_number") == 1).drop("row_number")

    return df_clean

