import os


def test_silver_outputs_exist():
    expected_dirs = [
        "data_lake/silver/adzuna_jobs_clean",
        "data_lake/silver/github_clean",
        "data_lake/silver/stackoverflow_clean",
        "data_lake/silver/streaming_jobs_spark",
    ]

    for directory in expected_dirs:
        assert os.path.isdir(directory), f"Silver manquant : {directory}"


def test_gold_outputs_exist():
    expected_dirs = [
        "data_lake/gold/jobs_by_location",
        "data_lake/gold/salary_by_location",
        "data_lake/gold/jobs_by_company",
        "data_lake/gold/github_top_languages",
        "data_lake/gold/github_top_repositories",
        "data_lake/gold/github_top_owners",
        "data_lake/gold/stackoverflow_developers_by_country",
        "data_lake/gold/stackoverflow_salary_by_country",
        "data_lake/gold/stackoverflow_remote_work",
        "data_lake/gold/stackoverflow_devtype",
    ]

    for directory in expected_dirs:
        assert os.path.isdir(directory), f"Gold manquant : {directory}"
