from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round

SILVER_PATH = "data_lake/silver/stackoverflow_clean"
GOLD_PATH = "data_lake/gold"

spark = (
    SparkSession.builder
    .appName("JobTech StackOverflow Silver to Gold")
    .getOrCreate()
)

df = spark.read.parquet(SILVER_PATH)

developers_by_country = (
    df.groupBy("country")
    .agg(count("*").alias("developer_count"))
    .orderBy(col("developer_count").desc())
)

salary_by_country = (
    df.filter(col("salary_yearly").isNotNull())
    .groupBy("country")
    .agg(round(avg("salary_yearly"), 2).alias("avg_salary"))
    .orderBy(col("avg_salary").desc())
)

remote_work_distribution = (
    df.groupBy("remote_work")
    .agg(count("*").alias("developer_count"))
    .orderBy(col("developer_count").desc())
)

developers_by_devtype = (
    df.groupBy("dev_type")
    .agg(count("*").alias("developer_count"))
    .orderBy(col("developer_count").desc())
)

developers_by_country.write.mode("overwrite").parquet(
    f"{GOLD_PATH}/stackoverflow_developers_by_country"
)

salary_by_country.write.mode("overwrite").parquet(
    f"{GOLD_PATH}/stackoverflow_salary_by_country"
)

remote_work_distribution.write.mode("overwrite").parquet(
    f"{GOLD_PATH}/stackoverflow_remote_work"
)

developers_by_devtype.write.mode("overwrite").parquet(
    f"{GOLD_PATH}/stackoverflow_devtype"
)

print("Gold StackOverflow créée avec succès")

spark.stop()
