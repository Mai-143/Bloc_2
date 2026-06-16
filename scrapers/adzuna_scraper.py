import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

RAW_DIR = "data_lake/raw/adzuna"
COLLECTION_DATE = datetime.now().strftime("%Y%m%d")

APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")

COUNTRY = "fr"
WHAT = "data engineer"
RESULTS_PER_PAGE = 50


def main():
    if not APP_ID or not APP_KEY:
        raise ValueError("ADZUNA_APP_ID et ADZUNA_APP_KEY doivent être définis dans .env")

    output_dir = os.path.join(RAW_DIR, COLLECTION_DATE)
    os.makedirs(output_dir, exist_ok=True)

    url = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search/1"

    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "what": WHAT,
        "results_per_page": RESULTS_PER_PAGE,
        "content-type": "application/json",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    output_file = os.path.join(output_dir, "adzuna_jobs.json")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ RAW Adzuna sauvegardé : {output_file}")


if __name__ == "__main__":
    main()