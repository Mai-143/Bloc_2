import os
import json
import time
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

RAW_DIR = "data_lake/raw/adzuna"
BRONZE_DIR = "data_lake/bronze"
COLLECTION_DATE = datetime.now().strftime("%Y%m%d")

APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")

COUNTRIES = ["fr", "gb", "de", "nl", "be", "us"]

SEARCHES = [
    "data engineer",
    "data analyst",
    "data scientist",
    "software engineer",
    "backend developer",
    "python developer",
    "machine learning engineer",
]

RESULTS_PER_PAGE = 50
NB_PAGES = 10
SLEEP_SECONDS = 1


def fetch_page(country: str, query: str, page: int):
    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"

    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "what": query,
        "results_per_page": RESULTS_PER_PAGE,
        "content-type": "application/json",
    }

    response = requests.get(url, params=params, timeout=30)

    if response.status_code == 429:
        print("⚠️ Limite API atteinte. Pause 60 secondes...")
        time.sleep(60)
        response = requests.get(url, params=params, timeout=30)

    response.raise_for_status()
    return response.json()


def main():
    if not APP_ID or not APP_KEY:
        raise ValueError("ADZUNA_APP_ID et ADZUNA_APP_KEY doivent être définis dans .env")

    output_dir = os.path.join(RAW_DIR, COLLECTION_DATE)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(BRONZE_DIR, exist_ok=True)

    all_results = []

    metadata = {
        "source": "adzuna",
        "collection_date": COLLECTION_DATE,
        "countries": COUNTRIES,
        "queries": SEARCHES,
        "results_per_page": RESULTS_PER_PAGE,
        "nb_pages_requested": NB_PAGES,
    }

    for country in COUNTRIES:
        for query in SEARCHES:
            for page in range(1, NB_PAGES + 1):
                print(f"📥 {country.upper()} | {query} | page {page}/{NB_PAGES}")

                try:
                    data = fetch_page(country, query, page)
                    results = data.get("results", [])

                    if not results:
                        print(f"⚠️ Aucun résultat : {country} | {query} | page {page}")
                        break

                    for job in results:
                        job["search_country"] = country
                        job["search_query"] = query
                        job["collection_date"] = COLLECTION_DATE

                    all_results.extend(results)

                    print(f"✅ {len(results)} offres récupérées")
                    time.sleep(SLEEP_SECONDS)

                except requests.exceptions.HTTPError as e:
                    print(f"❌ Erreur HTTP : {country} | {query} | page {page} | {e}")
                    break

                except requests.exceptions.RequestException as e:
                    print(f"❌ Erreur réseau : {country} | {query} | page {page} | {e}")
                    break

    output_data = {
        "metadata": metadata,
        "total_results": len(all_results),
        "results": all_results,
    }

    # Sauvegarde RAW JSON
    output_file = os.path.join(output_dir, "adzuna_jobs.json")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print("\n✅ RAW Adzuna sauvegardé :", output_file)
    print("✅ Nombre total d'offres récupérées :", len(all_results))

    # Sauvegarde BRONZE CSV
    df = pd.json_normalize(all_results)

    bronze_file = os.path.join(BRONZE_DIR, "adzuna_jobs.csv")
    df.to_csv(bronze_file, index=False, encoding="utf-8")

    print(f"✅ BRONZE Adzuna sauvegardé : {bronze_file}")
    print(f"✅ Nombre de lignes Bronze : {len(df)}")


if __name__ == "__main__":
    main()