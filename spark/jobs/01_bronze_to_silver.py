from pyspark.sql.functions import col, trim, lower, lit, expr
from spark.common.spark_session import get_spark_session
import pandas as pd

spark = get_spark_session("JobTech Bronze to Silver")

# =========================
# ADZUNA JOBS
# =========================

adzuna = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data_lake/bronze/adzuna_jobs.csv")
)

adzuna_silver = (
    adzuna
    .dropDuplicates()
    .filter(col("title").isNotNull())
    .filter(col("`company.display_name`").isNotNull())
    .select(
        trim(lower(col("title"))).alias("job_title"),
        trim(col("`company.display_name`")).alias("company"),
        trim(col("`location.display_name`")).alias("location"),
        expr("try_cast(salary_min as double)").alias("salary_min"),
        expr("try_cast(salary_max as double)").alias("salary_max"),
        trim(col("`category.label`")).alias("category"),
        trim(col("contract_type")).alias("contract_type"),
        trim(col("contract_time")).alias("contract_time"),
        col("created").alias("created_at"),
        lit("adzuna").alias("source")
    )
)

adzuna_silver.write.mode("overwrite").parquet("data_lake/silver/jobs")


# =========================
# GITHUB REPOSITORIES
# =========================

github = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data_lake/bronze/github_repositories.csv")
)

github_silver = (
    github
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
        trim(col("`owner.login`")).alias("owner"),
        lit("github").alias("source")
    )
)

github_silver.write.mode("overwrite").parquet("data_lake/silver/github")


# =========================
# STACKOVERFLOW SURVEY
# =========================

stackoverflow = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data_lake/bronze/stackoverflow_survey.csv")
)

stackoverflow_silver = (
    stackoverflow
    .filter(col("ResponseId").isNotNull())
    .select(
        col("ResponseId").alias("developer_id"),
        trim(col("Country")).alias("country"),
        trim(col("Employment")).alias("employment"),
        trim(col("RemoteWork")).alias("remote_work"),
        trim(col("DevType")).alias("dev_type"),
        trim(col("YearsCodePro")).alias("years_code_pro"),
        expr("try_cast(ConvertedCompYearly as double)").alias("salary_yearly"),
        col("LanguageHaveWorkedWith").alias("languages_used"),
        col("LanguageWantToWorkWith").alias("languages_wanted"),
        trim(col("Industry")).alias("industry"),
        lit("stackoverflow").alias("source")
    )
)

stackoverflow_silver.write.mode("overwrite").parquet("data_lake/silver/stackoverflow")


print("Silver Layer créée avec succès")
print(f"Jobs Adzuna : {adzuna_silver.count()}")
print(f"Repos GitHub : {github_silver.count()}")
print(f"Développeurs StackOverflow : {stackoverflow_silver.count()}")

spark.stop()
