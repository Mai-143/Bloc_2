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

RAW_DIR = "data_lake/raw/github"
BRONZE_DIR = "data_lake/bronze"
COLLECTION_DATE = datetime.now().strftime("%Y%m%d")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

LANGUAGES = [
    "python",
    "java",
    "javascript",
    "typescript",
    "go",
    "scala",
    "rust",
    "kotlin",
    "csharp",
    "php",
    "swift",
    "dart",
]

PER_PAGE = 100
NB_PAGES = 10
SLEEP_SECONDS = 2


def get_headers():
    headers = {
        "Accept": "application/vnd.github+json"
    }

    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    return headers


def fetch_github_repositories(language: str, page: int = 1, per_page: int = 100):
    url = "https://api.github.com/search/repositories"

    params = {
        "q": f"language:{language} stars:>10",
        "sort": "stars",
        "order": "desc",
        "per_page": per_page,
        "page": page,
    }

    response = requests.get(
        url,
        headers=get_headers(),
        params=params,
        timeout=30,
    )

    if response.status_code == 403:
        raise Exception(
            "Limite GitHub atteinte. Ajoute un GITHUB_TOKEN dans le fichier .env."
        )

    response.raise_for_status()
    return response.json()


def save_raw_data(data_by_language):
    output_dir = os.path.join(RAW_DIR, COLLECTION_DATE)
    os.makedirs(output_dir, exist_ok=True)

    for language, data in data_by_language.items():
        output_file = os.path.join(output_dir, f"github_{language}.json")

        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

        print(f"✅ RAW GitHub sauvegardé : {output_file}")


def save_bronze_csv(all_repositories):
    os.makedirs(BRONZE_DIR, exist_ok=True)

    if not all_repositories:
        print("⚠️ Aucun dépôt GitHub à sauvegarder en Bronze.")
        return

    df = pd.json_normalize(all_repositories)

    bronze_file = os.path.join(BRONZE_DIR, "github_repositories.csv")
    df.to_csv(bronze_file, index=False, encoding="utf-8")

    print(f"✅ BRONZE GitHub sauvegardé : {bronze_file}")
    print(f"✅ Nombre de lignes Bronze GitHub : {len(df)}")


def main():
    print("🚀 Démarrage collecte GitHub")

    if not GITHUB_TOKEN:
        print("⚠️ Aucun GITHUB_TOKEN trouvé dans .env. Les limites GitHub seront très basses.")

    all_repositories = []
    raw_by_language = {}

    for language in LANGUAGES:
        language_items = []

        for page in range(1, NB_PAGES + 1):
            try:
                print(f"🔎 Collecte : {language} | page {page}/{NB_PAGES}")

                data = fetch_github_repositories(
                    language=language,
                    page=page,
                    per_page=PER_PAGE,
                )

                repos = data.get("items", [])

                if not repos:
                    print(f"⚠️ Aucun dépôt trouvé : {language} | page {page}")
                    break

                for repo in repos:
                    repo["search_language"] = language
                    repo["collection_date"] = COLLECTION_DATE

                language_items.extend(repos)
                all_repositories.extend(repos)

                print(f"✅ {len(repos)} dépôts récupérés")

                time.sleep(SLEEP_SECONDS)

            except Exception as error:
                print(f"❌ Erreur pour {language} page {page} : {error}")
                break

        raw_by_language[language] = {
            "metadata": {
                "source": "github",
                "language": language,
                "collection_date": COLLECTION_DATE,
                "pages_requested": NB_PAGES,
                "per_page": PER_PAGE,
                "total_items_collected": len(language_items),
            },
            "items": language_items,
        }

    save_raw_data(raw_by_language)
    save_bronze_csv(all_repositories)

    print("✅ Collecte GitHub terminée")
    print(f"✅ Nombre total de dépôts récupérés : {len(all_repositories)}")


if __name__ == "__main__":
    main()