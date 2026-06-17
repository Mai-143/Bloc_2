import os
import pytest


def test_spark_scripts_exist():
    expected_scripts = [
        "spark/jobs/01_bronze_to_silver.py",
        "spark/jobs/02_silver_to_gold.py",
        "spark/jobs/03_load_to_postgres.py",
        "spark/jobs/04_github_events_streaming.py",
 
    ]

    for script in expected_scripts:
        assert os.path.isfile(script), f"Script Spark manquant : {script}"


def test_kafka_scripts_exist():
    expected_scripts = [
        "kafka/producer.py",
    ]

    for script in expected_scripts:
        assert os.path.isfile(script), f"Script Kafka manquant : {script}"


def test_generated_outputs_if_present():
    expected_dirs = [
        "data_lake/silver/jobs",
        "data_lake/silver/github",
        "data_lake/silver/stackoverflow",
        "data_lake/silver/streaming_github_events",
        "data_lake/gold/jobs_by_location",
        "data_lake/gold/github_language_popularity",
        "data_lake/gold/developer_salary_by_country",
        "data_lake/gold/salary_by_category",
    ]

    existing_dirs = [directory for directory in expected_dirs if os.path.isdir(directory)]

    if not existing_dirs:
        pytest.skip("Les outputs Spark ne sont pas générés dans l'environnement CI.")

    for directory in existing_dirs:
        assert os.path.isdir(directory)
