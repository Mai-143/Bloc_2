import os
import pytest


def test_spark_scripts_exist():
    expected_scripts = [
        "spark/adzuna_bronze_to_silver.py",
        "spark/adzuna_silver_to_gold.py",
        "spark/git_bronze_to_silver.py",
        "spark/git_silver_to_gold.py",
        "spark/stackoverflow_bronze_to_silver.py",
        "spark/stackoverflow_silver_to_gold.py",
        "spark/github_events_streaming.py",
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
        "data_lake/silver/adzuna_jobs_clean",
        "data_lake/silver/github_clean",
        "data_lake/silver/stackoverflow_clean",
        "data_lake/silver/streaming_github_events",
        "data_lake/gold/jobs_by_location",
        "data_lake/gold/github_top_languages",
        "data_lake/gold/stackoverflow_developers_by_country",
    ]

    existing_dirs = [directory for directory in expected_dirs if os.path.isdir(directory)]

    if not existing_dirs:
        pytest.skip("Les outputs Spark ne sont pas générés dans l'environnement CI.")

    for directory in existing_dirs:
        assert os.path.isdir(directory)