import os
import shutil
from datetime import datetime


SOURCE_FILE = "data_lake/raw/stackoverflow/survey_results_public.csv"
RAW_DIR = "data_lake/raw/stackoverflow"


def main():
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(
            f"Fichier introuvable : {SOURCE_FILE}. "
            "Télécharge survey_results_public.csv et place-le dans data_lake/raw/stackoverflow/"
        )

    today = datetime.now().strftime("%Y%m%d")
    output_dir = os.path.join(RAW_DIR, today)
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, "stackoverflow_survey.csv")
    shutil.copy(SOURCE_FILE, output_file)

    print(f"✅ StackOverflow Survey copié dans RAW : {output_file}")


if __name__ == "__main__":
    main()