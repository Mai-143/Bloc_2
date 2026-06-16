import os


def test_data_lake_layers_exist():
    expected_dirs = [
        "data_lake/raw",
        "data_lake/bronze",
        "data_lake/silver",
        "data_lake/gold",
    ]

    for directory in expected_dirs:
        assert os.path.isdir(directory), f"Dossier manquant : {directory}"


def test_bronze_files_exist():
    expected_files = [
        "data_lake/bronze/adzuna_jobs.csv",
        "data_lake/bronze/github_repositories.csv",
        "data_lake/bronze/stackoverflow_survey.csv",
    ]

    for file_path in expected_files:
        assert os.path.isfile(file_path), f"Fichier manquant : {file_path}"
