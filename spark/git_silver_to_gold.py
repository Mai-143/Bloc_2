from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, round

SILVER_PATH = "data_lake/silver/github_clean"
GOLD_PATH = "data_lake/gold"

spark = (
    SparkSession.builder
    .appName("JobTech GitHub Silver to Gold")
    .getOrCreate()
)

df = spark.read.parquet(SILVER_PATH)

top_languages = (
    df.groupBy("language")
    .agg(
        count("*").alias("repository_count"),
        sum("stars").alias("total_stars"),
        round(avg("stars"), 2).alias("avg_stars")
    )
    .orderBy(col("total_stars").desc())
)

top_repositories = (
    df.select(
        "repository_name",
        "full_name",
        "language",
        "stars",
        "forks",
        "open_issues",
        "owner",
        "html_url"
    )
    .orderBy(col("stars").desc())
)

top_owners = (
    df.groupBy("owner")
    .agg(
        count("*").alias("repository_count"),
        sum("stars").alias("total_stars")
    )
    .orderBy(col("total_stars").desc())
)

top_languages.write.mode("overwrite").parquet(f"{GOLD_PATH}/github_top_languages")
top_repositories.write.mode("overwrite").parquet(f"{GOLD_PATH}/github_top_repositories")
top_owners.write.mode("overwrite").parquet(f"{GOLD_PATH}/github_top_owners")

print("Gold GitHub créée avec succès")

spark.stop()