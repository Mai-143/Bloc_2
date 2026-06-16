from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, expr

BRONZE_PATH = "data_lake/bronze/github_repositories.csv"
SILVER_PATH = "data_lake/silver/github_clean"

spark = (
    SparkSession.builder
    .appName("JobTech GitHub Bronze to Silver")
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
    .dropDuplicates(["id"])
    .filter(col("name").isNotNull())
    .filter(col("language").isNotNull())
    .select(
        col("id").alias("repository_id"),
        trim(col("name")).alias("repository_name"),
        trim(col("full_name")).alias("full_name"),
        trim(lower(col("language"))).alias("language"),
        expr("try_cast(stargazers_count as int)").alias("stars"),
        expr("try_cast(forks_count as int)").alias("forks"),
        expr("try_cast(open_issues_count as int)").alias("open_issues"),
        col("created_at"),
        col("updated_at"),
        col("pushed_at"),
        col("html_url"),
        col("topics"),
        trim(col("`owner.login`")).alias("owner")
    )
)

df_clean.printSchema()
df_clean.show(5, truncate=False)

df_clean.write.mode("overwrite").parquet(SILVER_PATH)

print(f"Nombre de repositories GitHub : {df_clean.count()}")

spark.stop()