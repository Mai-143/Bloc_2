from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", "/opt/postgresql.jar")
        .config("spark.driver.extraClassPath", "/opt/postgresql.jar")
        .config("spark.executor.extraClassPath", "/opt/postgresql.jar")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )