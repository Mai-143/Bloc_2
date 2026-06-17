DROP TABLE IF EXISTS fact_jobs;
DROP TABLE IF EXISTS dim_company;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_source;
DROP TABLE IF EXISTS dim_category;

DROP TABLE IF EXISTS gold_jobs_by_location;
DROP TABLE IF EXISTS gold_salary_by_category;
DROP TABLE IF EXISTS gold_github_language_popularity;
DROP TABLE IF EXISTS gold_developer_salary_by_country;

DROP TABLE IF EXISTS github_events_stream;
DROP TABLE IF EXISTS github_event_type_stats;
DROP TABLE IF EXISTS github_repo_activity;


CREATE TABLE dim_company (
    company_id SERIAL PRIMARY KEY,
    company_name TEXT UNIQUE
);

CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    location_name TEXT UNIQUE
);

CREATE TABLE dim_source (
    source_id SERIAL PRIMARY KEY,
    source_name TEXT UNIQUE
);

CREATE TABLE dim_category (
    category_id SERIAL PRIMARY KEY,
    category_name TEXT UNIQUE
);

CREATE TABLE fact_jobs (
    job_id SERIAL PRIMARY KEY,
    job_title TEXT,
    company_id INTEGER REFERENCES dim_company(company_id),
    location_id INTEGER REFERENCES dim_location(location_id),
    source_id INTEGER REFERENCES dim_source(source_id),
    category_id INTEGER REFERENCES dim_category(category_id),
    salary_min DOUBLE PRECISION,
    salary_max DOUBLE PRECISION,
    contract_type TEXT,
    contract_time TEXT,
    created_at TEXT
);


CREATE TABLE gold_jobs_by_location (
    location TEXT,
    job_count INTEGER
);

CREATE TABLE gold_salary_by_category (
    category TEXT,
    job_count INTEGER,
    avg_salary_min DOUBLE PRECISION,
    avg_salary_max DOUBLE PRECISION,
    min_salary DOUBLE PRECISION,
    max_salary DOUBLE PRECISION
);

CREATE TABLE gold_github_language_popularity (
    language TEXT,
    repo_count INTEGER,
    avg_stars DOUBLE PRECISION,
    max_stars INTEGER
);

CREATE TABLE gold_developer_salary_by_country (
    country TEXT,
    developer_count INTEGER,
    avg_salary_yearly DOUBLE PRECISION
);


CREATE TABLE github_events_stream (
    event_id TEXT,
    event_type TEXT,
    repo_name TEXT,
    actor_login TEXT,
    created_at TEXT,
    source TEXT
);

CREATE TABLE github_event_type_stats (
    event_type TEXT,
    event_count INTEGER
);

CREATE TABLE github_repo_activity (
    repo_name TEXT,
    event_count INTEGER
);