import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType


KAFKA_SERVER = "kafka:29092"
KAFKA_TOPIC = "github_events"

OUTPUT_PATH = "data_lake/silver/streaming_github_events"
CHECKPOINT_PATH = "data_lake/checkpoints/github_events"


schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("repo_name", StringType(), True),
    StructField("actor_login", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("collected_at", StringType(), True),
])


def main():
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("GitHubEventsStreaming")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    events_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .withColumn("processed_at", current_timestamp())
        .filter(col("event_id").isNotNull())
    )

    query = (
        events_df.writeStream
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )

    print("Spark Streaming GitHub Events démarré...")
    query.awaitTermination()


if __name__ == "__main__":
    main()