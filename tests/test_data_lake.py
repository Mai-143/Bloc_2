import os


def test_project_directories_exist():
    expected_dirs = [
        "api",
        "scrapers",
        "spark",
        "kafka",
        "warehouse",
        "tests",
    ]

    for directory in expected_dirs:
        assert os.path.isdir(directory), f"Dossier manquant : {directory}"


def test_data_lake_base_structure_exists():
    expected_dirs = [
        "data_lake",
        "data_lake/raw",
        "data_lake/bronze",
        "data_lake/silver",
        "data_lake/gold",
    ]

    for directory in expected_dirs:
        assert os.path.isdir(directory), f"Dossier Data Lake manquant : {directory}"


def test_main_project_files_exist():
    expected_files = [
        "Dockerfile",
        "docker-compose.yml",
        "requirements.txt",
        "api/main.py",
        "warehouse/schema.sql",
    ]

    for file_path in expected_files:
        assert os.path.isfile(file_path), f"Fichier manquant : {file_path}"