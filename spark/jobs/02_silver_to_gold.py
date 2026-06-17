from pyspark.sql.functions import col, count, avg, max, min
from spark.common.spark_session import get_spark_session


spark = get_spark_session("JobTech Silver to Gold")


jobs = spark.read.parquet("data_lake/silver/jobs")
github = spark.read.parquet("data_lake/silver/github")
stackoverflow = spark.read.parquet("data_lake/silver/stackoverflow")


# KPI 1 : offres par localisation
jobs_by_location = (
    jobs
    .groupBy("location")
    .agg(count("*").alias("job_count"))
    .orderBy(col("job_count").desc())
)

jobs_by_location.write.mode("overwrite").parquet("data_lake/gold/jobs_by_location")


# KPI 2 : salaires par catégorie
salary_by_category = (
    jobs
    .filter(col("salary_min").isNotNull())
    .groupBy("category")
    .agg(
        count("*").alias("job_count"),
        avg("salary_min").alias("avg_salary_min"),
        avg("salary_max").alias("avg_salary_max"),
        min("salary_min").alias("min_salary"),
        max("salary_max").alias("max_salary")
    )
    .orderBy(col("job_count").desc())
)

salary_by_category.write.mode("overwrite").parquet("data_lake/gold/salary_by_category")


# KPI 3 : technologies les plus populaires GitHub
github_language_popularity = (
    github
    .groupBy("language")
    .agg(
        count("*").alias("repo_count"),
        avg("stars").alias("avg_stars"),
        max("stars").alias("max_stars")
    )
    .orderBy(col("repo_count").desc())
)

github_language_popularity.write.mode("overwrite").parquet("data_lake/gold/github_language_popularity")


# KPI 4 : salaires développeurs par pays StackOverflow
developer_salary_by_country = (
    stackoverflow
    .filter(col("salary_yearly").isNotNull())
    .groupBy("country")
    .agg(
        count("*").alias("developer_count"),
        avg("salary_yearly").alias("avg_salary_yearly")
    )
    .orderBy(col("developer_count").desc())
)

developer_salary_by_country.write.mode("overwrite").parquet("data_lake/gold/developer_salary_by_country")


print("Gold Layer créée avec succès")
print(f"Jobs par localisation : {jobs_by_location.count()}")
print(f"Salaires par catégorie : {salary_by_category.count()}")
print(f"Langages GitHub : {github_language_popularity.count()}")
print(f"Salaires développeurs par pays : {developer_salary_by_country.count()}")

spark.stop()
