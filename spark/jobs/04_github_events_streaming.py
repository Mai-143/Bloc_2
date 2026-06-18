from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("GitHubEventsStreaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.3"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("repo_name", StringType(), True),
    StructField("actor_login", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("source", StringType(), True),
])

POSTGRES_URL = "jdbc:postgresql://postgres:5432/jobtech"
POSTGRES_PROPERTIES = {
    "user": "jobtech_user",
    "password": "jobtech_pass",
    "driver": "org.postgresql.Driver"
}

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "github_events") \
    .option("startingOffsets", "earliest") \
    .load()

df_events = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("event_id").isNotNull())


def write_events_to_postgres(batch_df, batch_id):
    batch_df.write \
        .mode("append") \
        .jdbc(
            url=POSTGRES_URL,
            table="github_events_stream",
            properties=POSTGRES_PROPERTIES
        )


def write_event_type_stats_to_postgres(batch_df, batch_id):
    batch_df.write \
        .mode("overwrite") \
        .jdbc(
            url=POSTGRES_URL,
            table="github_event_type_stats",
            properties=POSTGRES_PROPERTIES
        )


def write_repo_activity_to_postgres(batch_df, batch_id):
    batch_df.write \
        .mode("overwrite") \
        .jdbc(
            url=POSTGRES_URL,
            table="github_repo_activity",
            properties=POSTGRES_PROPERTIES
        )


query_raw = df_events.writeStream \
    .format("parquet") \
    .option("path", "data_lake/silver/streaming_github_events") \
    .option("checkpointLocation", "data_lake/silver/checkpoints/github_events_raw") \
    .outputMode("append") \
    .start()

query_postgres = df_events.writeStream \
    .foreachBatch(write_events_to_postgres) \
    .option("checkpointLocation", "data_lake/silver/checkpoints/github_events_postgres") \
    .outputMode("append") \
    .start()

event_type_stats = df_events.groupBy("event_type") \
    .agg(count("*").alias("event_count"))

query_event_type = event_type_stats.writeStream \
    .foreachBatch(write_event_type_stats_to_postgres) \
    .option("checkpointLocation", "data_lake/silver/checkpoints/github_event_type_stats_postgres") \
    .outputMode("complete") \
    .start()

repo_activity = df_events.groupBy("repo_name") \
    .agg(count("*").alias("event_count"))

query_repo_activity = repo_activity.writeStream \
    .foreachBatch(write_repo_activity_to_postgres) \
    .option("checkpointLocation", "data_lake/silver/checkpoints/github_repo_activity_postgres") \
    .outputMode("complete") \
    .start()

print("Streaming GitHub lancé.")
print("Kafka -> Spark Streaming -> Parquet Silver -> PostgreSQL")
print("Lance le producer Kafka dans un autre terminal.")

spark.streams.awaitAnyTermination()