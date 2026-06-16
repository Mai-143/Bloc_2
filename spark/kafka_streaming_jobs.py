from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType

TOPIC = "job_offers_stream"
BOOTSTRAP_SERVERS = "kafka:29092"
OUTPUT_PATH = "data_lake/silver/streaming_jobs_spark"
CHECKPOINT_PATH = "data_lake/silver/checkpoints/jobs_stream"

spark = (
    SparkSession.builder
    .appName("JobTech Kafka Spark Streaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0"
    )
    .getOrCreate()
)

schema = StructType([
    StructField("title", StringType(), True),
    StructField("company", StringType(), True),
    StructField("location", StringType(), True),
    StructField("salary_min", StringType(), True),
    StructField("salary_max", StringType(), True),
    StructField("category", StringType(), True),
    StructField("created", StringType(), True),
    StructField("source", StringType(), True),
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
    .withColumn("salary_min", expr("try_cast(salary_min as double)"))
    .withColumn("salary_max", expr("try_cast(salary_max as double)"))
)

query = (
    parsed_df.writeStream
    .format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("Spark Streaming terminé")
spark.stop()
