from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, expr

BRONZE_PATH = "data_lake/bronze/stackoverflow_survey.csv"
SILVER_PATH = "data_lake/silver/stackoverflow_clean"

spark = (
    SparkSession.builder
    .appName("JobTech StackOverflow Bronze to Silver")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(BRONZE_PATH)
)

df_clean = (
    df
    .filter(col("ResponseId").isNotNull())
    .select(
        col("ResponseId").alias("developer_id"),
        trim(col("Country")).alias("country"),
        trim(col("Employment")).alias("employment"),
        trim(col("RemoteWork")).alias("remote_work"),
        trim(col("DevType")).alias("dev_type"),
        trim(col("YearsCodePro")).alias("years_code_pro"),
        expr("try_cast(CompTotal as double)").alias("comp_total"),
        expr("try_cast(ConvertedCompYearly as double)").alias("salary_yearly"),
        col("LanguageHaveWorkedWith").alias("languages_used"),
        col("LanguageWantToWorkWith").alias("languages_wanted"),
        trim(col("Industry")).alias("industry"),
        trim(col("ProfessionalCloud")).alias("cloud_usage")
    )
)

df_clean.printSchema()
df_clean.show(5, truncate=False)

df_clean.write.mode("overwrite").parquet(SILVER_PATH)

print(f"Nombre de développeurs : {df_clean.count()}")

spark.stop()

