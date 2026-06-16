from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, lit, expr

BRONZE_PATH = "data_lake/bronze/adzuna_jobs.csv"
SILVER_PATH = "data_lake/silver/adzuna_jobs_clean"

spark = (
    SparkSession.builder
    .appName("JobTech Bronze to Silver")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(BRONZE_PATH)
)

df.printSchema()

df_clean = (
    df
    .dropDuplicates()
    .filter(col("title").isNotNull())
    .filter(col("`company.display_name`").isNotNull())
    .select(
        trim(lower(col("title"))).alias("title"),
        trim(col("`company.display_name`")).alias("company"),
        trim(col("`location.display_name`")).alias("location"),
        expr("try_cast(salary_min as double)").alias("salary_min"),
        expr("try_cast(salary_max as double)").alias("salary_max"),
        col("`category.label`").alias("category"),
        col("contract_type"),
        col("contract_time"),
        col("created"),
        lit("adzuna").alias("source")
    )
)

df_clean.printSchema()
df_clean.show(5, truncate=False)

(
    df_clean.write
    .mode("overwrite")
    .parquet(SILVER_PATH)
)

print(f"Nombre d'offres : {df_clean.count()}")

spark.stop()
