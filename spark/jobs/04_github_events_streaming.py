from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("GitHubEventsStreaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("repo_name", StringType(), True),
    StructField("language", StringType(), True),
    StructField("created_at", StringType(), True),
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "github_events") \
    .option("startingOffsets", "earliest") \
    .load()

df_events = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query_raw = df_events.writeStream \
    .format("parquet") \
    .option("path", "data_lake/silver/streaming_github_events") \
    .option("checkpointLocation", "data_lake/silver/checkpoints/github_events_raw") \
    .outputMode("append") \
    .start()

event_type_stats = df_events.groupBy("event_type") \
    .agg(count("*").alias("event_count"))

query_event_type = event_type_stats.writeStream \
    .format("memory") \
    .queryName("github_event_type_stats") \
    .outputMode("complete") \
    .start()

repo_activity = df_events.groupBy("repo_name") \
    .agg(count("*").alias("event_count"))

query_repo_activity = repo_activity.writeStream \
    .format("memory") \
    .queryName("github_repo_activity") \
    .outputMode("complete") \
    .start()

print("Streaming GitHub lancé.")
print("Lance le producer Kafka dans un autre terminal.")
print("Les événements bruts sont écrits dans data_lake/silver/streaming_github_events.")

query_raw.awaitTermination()