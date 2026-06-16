DROP TABLE IF EXISTS fact_jobs;
DROP TABLE IF EXISTS jobs_source_stats;
DROP TABLE IF EXISTS jobs_location_stats;
DROP TABLE IF EXISTS github_language_stats;
DROP TABLE IF EXISTS stackoverflow_country_stats;
DROP TABLE IF EXISTS stackoverflow_skills_stats;
DROP TABLE IF EXISTS stackoverflow_profile_stats;
DROP TABLE IF EXISTS github_events_stream;
DROP TABLE IF EXISTS github_event_type_stats;
DROP TABLE IF EXISTS github_repo_activity;

CREATE TABLE fact_jobs (
    id SERIAL PRIMARY KEY,
    job_id TEXT,
    title TEXT,
    company TEXT,
    location TEXT,
    category TEXT,
    description TEXT,
    contract_type TEXT,
    redirect_url TEXT,
    created_at TEXT,
    salary_min NUMERIC,
    salary_max NUMERIC,
    source TEXT,
    collection_date TEXT,
    skills TEXT,
    rating NUMERIC,
    salary_estimate TEXT
);

CREATE TABLE jobs_source_stats (
    source TEXT PRIMARY KEY,
    jobs_count INTEGER,
    avg_salary_min NUMERIC,
    avg_salary_max NUMERIC,
    avg_rating NUMERIC
);

CREATE TABLE jobs_location_stats (
    location TEXT PRIMARY KEY,
    jobs_count INTEGER,
    avg_salary_min NUMERIC,
    avg_salary_max NUMERIC
);

CREATE TABLE github_language_stats (
    language TEXT PRIMARY KEY,
    repositories_count INTEGER,
    total_stars INTEGER,
    avg_stars NUMERIC,
    total_forks INTEGER,
    total_open_issues INTEGER
);

CREATE TABLE stackoverflow_country_stats (
    country TEXT PRIMARY KEY,
    respondents_count INTEGER,
    avg_years_code NUMERIC,
    avg_years_code_pro NUMERIC,
    avg_salary_yearly_usd NUMERIC
);

CREATE TABLE stackoverflow_skills_stats (
    skill_category TEXT,
    skill_name TEXT,
    respondents_count INTEGER
);

CREATE TABLE stackoverflow_profile_stats (
    employment TEXT,
    remote_work TEXT,
    respondents_count INTEGER,
    avg_salary_yearly_usd NUMERIC,
    avg_years_code_pro NUMERIC
);

CREATE TABLE github_events_stream (
    event_id TEXT PRIMARY KEY,
    event_type TEXT,
    repo_name TEXT,
    actor_login TEXT,
    created_at TEXT,
    collected_at TEXT,
    processed_at TEXT
);

CREATE TABLE github_event_type_stats (
    event_type TEXT PRIMARY KEY,
    events_count INTEGER
);

CREATE TABLE github_repo_activity (
    repo_name TEXT PRIMARY KEY,
    events_count INTEGER,
    last_event_at TEXT
);