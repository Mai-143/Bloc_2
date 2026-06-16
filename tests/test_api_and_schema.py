import os


def read_file(path):
    with open(path, "r", encoding="utf-8") as file:
        return file.read()


def test_api_file_contains_required_routes():
    content = read_file("api/main.py")

    expected_routes = [
        "/",
        "/jobs",
        "/streaming/github/events",
        "/streaming/github/event-types",
        "/streaming/github/repos",
    ]

    for route in expected_routes:
        assert route in content, f"Route API manquante : {route}"


def test_schema_contains_required_tables():
    content = read_file("warehouse/schema.sql")

    expected_tables = [
        "fact_jobs",
        "github_events_stream",
        "github_event_type_stats",
        "github_repo_activity",
    ]

    for table in expected_tables:
        assert table in content, f"Table PostgreSQL manquante : {table}"


def test_requirements_contains_required_packages():
    content = read_file("requirements.txt")

    expected_packages = [
        "pandas",
        "numpy",
        "SQLAlchemy",
        "fastapi",
        "uvicorn",
        "pytest",
        "pyspark",
        "kafka-python",
        "psycopg2-binary",
        "pyarrow",
    ]

    for package in expected_packages:
        assert package in content, f"Package manquant dans requirements.txt : {package}"