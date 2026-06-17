from fastapi import FastAPI, Query
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

app = FastAPI(
    title="JobTech Data Lake API",
    description="API exposant les données analytiques du Data Lake JobTech",
    version="1.0.0"
)

engine = create_engine(
    "postgresql+psycopg2://jobtech_user:jobtech_pass@postgres:5432/jobtech"
)


def records(query: str):
    df = pd.read_sql(query, engine)
    df = df.replace({np.nan: None})
    return df.to_dict(orient="records")


@app.get("/")
def healthcheck():
    return {
        "status": "running",
        "project": "JobTech Data Lake",
        "layers": ["bronze", "silver", "gold", "warehouse"]
    }


@app.get("/jobs")
def get_jobs(limit: int = Query(10, ge=1, le=100)):
    return records(f"""
        SELECT
            f.job_id,
            f.job_title,
            c.company_name AS company,
            l.location_name AS location,
            s.source_name AS source,
            cat.category_name AS category,
            f.salary_min,
            f.salary_max,
            f.contract_type,
            f.contract_time,
            f.created_at
        FROM fact_jobs f
        LEFT JOIN dim_company c ON f.company_id = c.company_id
        LEFT JOIN dim_location l ON f.location_id = l.location_id
        LEFT JOIN dim_source s ON f.source_id = s.source_id
        LEFT JOIN dim_category cat ON f.category_id = cat.category_id
        LIMIT {limit}
    """)


@app.get("/analytics/jobs-by-location")
def jobs_by_location():
    return records("""
        SELECT *
        FROM gold_jobs_by_location
        ORDER BY job_count DESC
    """)


@app.get("/analytics/salary-by-category")
def salary_by_category():
    return records("""
        SELECT *
        FROM gold_salary_by_category
        ORDER BY job_count DESC
    """)


@app.get("/analytics/github-languages")
def github_languages():
    return records("""
        SELECT *
        FROM gold_github_language_popularity
        ORDER BY repo_count DESC
    """)


@app.get("/analytics/developer-salary-by-country")
def developer_salary_by_country():
    return records("""
        SELECT *
        FROM gold_developer_salary_by_country
        ORDER BY developer_count DESC
    """)

@app.get("/streaming/github/events")
def github_streaming_events():
    return records("""
        SELECT *
        FROM github_events_stream
        LIMIT 100
    """)


@app.get("/streaming/github/event-types")
def github_event_types():
    return records("""
        SELECT *
        FROM github_event_type_stats
        ORDER BY event_count DESC
    """)


@app.get("/streaming/github/repos")
def github_repo_activity():
    return records("""
        SELECT *
        FROM github_repo_activity
        ORDER BY event_count DESC
    """)

@app.get("/warehouse/companies")
def get_companies():
    return records("""
        SELECT *
        FROM dim_company
        ORDER BY company_name
    """)


@app.get("/warehouse/locations")
def get_locations():
    return records("""
        SELECT *
        FROM dim_location
        ORDER BY location_name
    """)


@app.get("/warehouse/sources")
def get_sources():
    return records("""
        SELECT *
        FROM dim_source
        ORDER BY source_name
    """)


@app.get("/warehouse/categories")
def get_categories():
    return records("""
        SELECT *
        FROM dim_category
        ORDER BY category_name
    """)