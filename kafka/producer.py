import json
import time
import pandas as pd
from kafka import KafkaProducer

TOPIC = "job_offers_stream"
BOOTSTRAP_SERVERS = "kafka:29092"
INPUT_FILE = "data_lake/bronze/adzuna_jobs.csv"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

df = pd.read_csv(INPUT_FILE)

for _, row in df.iterrows():
    offer = {
        "title": row.get("title"),
        "company": row.get("company.display_name"),
        "location": row.get("location.display_name"),
        "salary_min": row.get("salary_min"),
        "salary_max": row.get("salary_max"),
        "category": row.get("category.label"),
        "created": row.get("created"),
        "source": "adzuna_stream"
    }

    producer.send(TOPIC, offer)
    print(f"Sent: {offer['title']} - {offer['company']}")
    time.sleep(0.5)

producer.flush()
producer.close()

print("Producer terminé")
