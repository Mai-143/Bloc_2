from fastapi import FastAPI
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

app = FastAPI(
    title="JobTech Data Lake API",
    description="API pour exposer les données du Data Warehouse JobTech",
    version="1.0.0"
)

DB_USER = "jobtech_user"
DB_PASSWORD = "jobtech_pass"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "jobtech"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def clean_records(df):
    df = df.replace({np.nan: None})
    return df.to_dict(orient="records")

@app.get("/")
def home():
    return {"message": "JobTech API is running"}

@app.get("/jobs")
def get_jobs(limit: int = 10):
    query = """
        SELECT job_id, title, company, location, salary_min, salary_max, source
        FROM fact_jobs
        LIMIT %(limit)s
    """
    df = pd.read_sql(query, engine, params={"limit": limit})
    return clean_records(df)

@app.get("/jobs/top-locations")
def get_top_locations(limit: int = 10):
    query = """
        SELECT location, jobs_count, avg_salary_min, avg_salary_max
        FROM jobs_location_stats
        ORDER BY jobs_count DESC
        LIMIT %(limit)s
    """
    df = pd.read_sql(query, engine, params={"limit": limit})
    return clean_records(df)

@app.get("/jobs/sources")
def get_sources():
    query = """
        SELECT source, jobs_count, avg_salary_min, avg_salary_max, avg_rating
        FROM jobs_source_stats
        ORDER BY jobs_count DESC
    """
    df = pd.read_sql(query, engine)
    return clean_records(df)

@app.get("/github/languages")
def get_github_languages():
    query = """
        SELECT language, repositories_count, total_stars, avg_stars
        FROM github_language_stats
        ORDER BY total_stars DESC
    """
    df = pd.read_sql(query, engine)
    return clean_records(df)

@app.get("/stackoverflow/top-skills")
def get_stackoverflow_skills(limit: int = 20):
    query = f"""
        SELECT skill_category, skill_name, respondents_count
        FROM stackoverflow_skills_stats
        ORDER BY respondents_count DESC
        LIMIT {limit}
    """
    df = pd.read_sql(query, engine)
    return clean_records(df)

@app.get("/stackoverflow/countries")
def get_stackoverflow_countries(limit: int = 10):
    query = f"""
        SELECT country, respondents_count, avg_salary_yearly_usd
        FROM stackoverflow_country_stats
        ORDER BY respondents_count DESC
        LIMIT {limit}
    """
    df = pd.read_sql(query, engine)
    return clean_records(df)

@app.get("/streaming/github/events")
def get_github_events(limit: int = 20):
    query = f"""
        SELECT event_id, event_type, repo_name, actor_login, created_at, collected_at, processed_at
        FROM github_events_stream
        ORDER BY created_at DESC
        LIMIT {limit}
    """
    return clean_records(pd.read_sql(query, engine))


@app.get("/streaming/github/event-types")
def get_github_event_types():
    query = """
        SELECT event_type, events_count
        FROM github_event_type_stats
        ORDER BY events_count DESC
    """
    return clean_records(pd.read_sql(query, engine))


@app.get("/streaming/github/repos")
def get_github_repo_activity(limit: int = 20):
    query = f"""
        SELECT repo_name, events_count, last_event_at
        FROM github_repo_activity
        ORDER BY events_count DESC
        LIMIT {limit}
    """
    return clean_records(pd.read_sql(query, engine))