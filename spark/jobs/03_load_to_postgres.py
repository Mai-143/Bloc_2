from pyspark.sql.functions import col
from spark.common.spark_session import get_spark_session


spark = get_spark_session("JobTech Load Dimensional Warehouse")

POSTGRES_URL = "jdbc:postgresql://postgres:5432/jobtech"
POSTGRES_PROPERTIES = {
    "user": "jobtech_user",
    "password": "jobtech_pass",
    "driver": "org.postgresql.Driver"
}


def write_table(df, table_name, mode="append"):
    (
        df.write
        .mode(mode)
        .jdbc(
            url=POSTGRES_URL,
            table=table_name,
            properties=POSTGRES_PROPERTIES
        )
    )


jobs = spark.read.parquet("data_lake/silver/jobs")

jobs_by_location = spark.read.parquet("data_lake/gold/jobs_by_location")
salary_by_category = spark.read.parquet("data_lake/gold/salary_by_category")
github_language_popularity = spark.read.parquet("data_lake/gold/github_language_popularity")
developer_salary_by_country = spark.read.parquet("data_lake/gold/developer_salary_by_country")


dim_company = (
    jobs
    .select(col("company").alias("company_name"))
    .filter(col("company_name").isNotNull())
    .dropDuplicates()
)

dim_location = (
    jobs
    .select(col("location").alias("location_name"))
    .filter(col("location_name").isNotNull())
    .dropDuplicates()
)

dim_source = (
    jobs
    .select(col("source").alias("source_name"))
    .filter(col("source_name").isNotNull())
    .dropDuplicates()
)

dim_category = (
    jobs
    .select(col("category").alias("category_name"))
    .filter(col("category_name").isNotNull())
    .dropDuplicates()
)


write_table(dim_company, "dim_company")
write_table(dim_location, "dim_location")
write_table(dim_source, "dim_source")
write_table(dim_category, "dim_category")


dim_company_db = spark.read.jdbc(
    POSTGRES_URL, "dim_company", properties=POSTGRES_PROPERTIES
)

dim_location_db = spark.read.jdbc(
    POSTGRES_URL, "dim_location", properties=POSTGRES_PROPERTIES
)

dim_source_db = spark.read.jdbc(
    POSTGRES_URL, "dim_source", properties=POSTGRES_PROPERTIES
)

dim_category_db = spark.read.jdbc(
    POSTGRES_URL, "dim_category", properties=POSTGRES_PROPERTIES
)


fact_jobs = (
    jobs
    .join(dim_company_db, jobs.company == dim_company_db.company_name, "left")
    .join(dim_location_db, jobs.location == dim_location_db.location_name, "left")
    .join(dim_source_db, jobs.source == dim_source_db.source_name, "left")
    .join(dim_category_db, jobs.category == dim_category_db.category_name, "left")
    .select(
        col("job_title"),
        col("company_id"),
        col("location_id"),
        col("source_id"),
        col("category_id"),
        col("salary_min"),
        col("salary_max"),
        col("contract_type"),
        col("contract_time"),
        col("created_at")
    )
)

write_table(fact_jobs, "fact_jobs")

write_table(jobs_by_location, "gold_jobs_by_location")
write_table(salary_by_category, "gold_salary_by_category")
write_table(github_language_popularity, "gold_github_language_popularity")
write_table(developer_salary_by_country, "gold_developer_salary_by_country")

print("Data Warehouse dimensionnel chargé avec succès")
print(f"dim_company : {dim_company.count()}")
print(f"dim_location : {dim_location.count()}")
print(f"dim_source : {dim_source.count()}")
print(f"dim_category : {dim_category.count()}")
print(f"fact_jobs : {fact_jobs.count()}")

spark.stop()