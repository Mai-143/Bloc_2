import os
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

RAW_DIR = "data_lake/raw/github"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")


def get_headers():
    headers = {
        "Accept": "application/vnd.github+json"
    }

    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    return headers


def fetch_github_repositories(language, per_page=30):
    url = "https://api.github.com/search/repositories"

    params = {
        "q": f"language:{language} stars:>100",
        "sort": "stars",
        "order": "desc",
        "per_page": per_page
    }

    response = requests.get(
        url,
        headers=get_headers(),
        params=params,
        timeout=30
    )

    if response.status_code == 403:
        raise Exception(
            "Limite GitHub atteinte. Ajoute un GITHUB_TOKEN dans le fichier .env."
        )

    response.raise_for_status()
    return response.json()


def save_raw_data(data, language):
    today = datetime.now().strftime("%Y%m%d")
    output_dir = os.path.join(RAW_DIR, today)
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, f"github_{language}.json")

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    print(f"Fichier sauvegardé : {output_file}")


def main():
    languages = ["python", "java", "javascript", "typescript", "go", "scala"]

    print("🚀 Démarrage collecte GitHub")

    if not GITHUB_TOKEN:
        print("⚠️ Aucun GITHUB_TOKEN trouvé dans .env. Les limites GitHub seront très basses.")

    for language in languages:
        try:
            print(f"🔎 Collecte : {language}")
            data = fetch_github_repositories(language)
            save_raw_data(data, language)
            time.sleep(2)

        except Exception as error:
            print(f"Erreur pour {language} : {error}")

    print("Collecte GitHub terminée")


if __name__ == "__main__":
    main()