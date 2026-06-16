import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

DB_USER = "jobtech_user"
DB_PASSWORD = "jobtech_pass"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "jobtech"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


def clean_df(df):
    df = df.replace({np.nan: None})
    return df


def read_parquet(path):
    if not os.path.exists(path):
        print(f"Fichier absent : {path}")
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_table(df, table_name, mode="replace"):
    if df.empty:
        print(f"Aucune donnée à charger dans {table_name}")
        return

    df = clean_df(df)
    df.to_sql(table_name, engine, if_exists=mode, index=False)
    print(f"Table chargée : {table_name} ({len(df)} lignes)")


def main():
    print("Chargement PostgreSQL...")

    fact_jobs = pd.read_csv("data_lake/gold/fact_jobs.csv")
    load_table(fact_jobs, "fact_jobs")

    jobs_location = read_parquet("data_lake/gold/jobs_by_location")
    load_table(jobs_location, "jobs_location_stats")

    github_languages = read_parquet("data_lake/gold/github_top_languages")
    load_table(github_languages, "github_language_stats")

    stackoverflow_countries = read_parquet("data_lake/gold/stackoverflow_developers_by_country")
    load_table(stackoverflow_countries, "stackoverflow_country_stats")

    github_events = read_parquet("data_lake/silver/streaming_github_events")
    load_table(github_events, "github_events_stream")

    if not github_events.empty:
        event_type_stats = (
            github_events
            .groupby("event_type")
            .size()
            .reset_index(name="events_count")
            .sort_values("events_count", ascending=False)
        )
        load_table(event_type_stats, "github_event_type_stats")

        repo_activity = (
            github_events
            .groupby("repo_name")
            .agg(
                events_count=("event_id", "count"),
                last_event_at=("created_at", "max")
            )
            .reset_index()
            .sort_values("events_count", ascending=False)
        )
        load_table(repo_activity, "github_repo_activity")

    print("Chargement terminé.")


if __name__ == "__main__":
    main()