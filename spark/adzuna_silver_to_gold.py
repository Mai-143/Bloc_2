from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round

SILVER_PATH = "data_lake/silver/adzuna_jobs_clean"
GOLD_PATH = "data_lake/gold"

spark = (
    SparkSession.builder
    .appName("JobTech Silver to Gold")
    .getOrCreate()
)

df = spark.read.parquet(SILVER_PATH)

jobs_by_location = (
    df.groupBy("location")
    .agg(count("*").alias("job_count"))
    .orderBy(col("job_count").desc())
)

salary_by_location = (
    df.filter(col("salary_min").isNotNull() & col("salary_max").isNotNull())
    .withColumn("avg_salary", (col("salary_min") + col("salary_max")) / 2)
    .groupBy("location")
    .agg(round(avg("avg_salary"), 2).alias("avg_salary"))
    .orderBy(col("avg_salary").desc())
)

jobs_by_company = (
    df.groupBy("company")
    .agg(count("*").alias("job_count"))
    .orderBy(col("job_count").desc())
)

jobs_by_location.write.mode("overwrite").parquet(f"{GOLD_PATH}/jobs_by_location")
salary_by_location.write.mode("overwrite").parquet(f"{GOLD_PATH}/salary_by_location")
jobs_by_company.write.mode("overwrite").parquet(f"{GOLD_PATH}/jobs_by_company")

print("Gold Layer créée avec succès")
spark.stop()
