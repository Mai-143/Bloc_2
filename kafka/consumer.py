import json
import os
from datetime import datetime
from kafka import KafkaConsumer
import pandas as pd

TOPIC = "job_offers_stream"
BOOTSTRAP_SERVERS = "kafka:29092"
OUTPUT_DIR = "data_lake/silver/streaming_jobs"

os.makedirs(OUTPUT_DIR, exist_ok=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id="jobtech-streaming-consumer",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=10000
)

records = []

for message in consumer:
    data = message.value
    records.append(data)
    print(f"Received: {data.get('title')} - {data.get('company')}")

if records:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{OUTPUT_DIR}/streaming_jobs_{timestamp}.csv"

    df = pd.DataFrame(records)
    df.to_csv(output_file, index=False)

    print(f"{len(records)} messages sauvegardés dans {output_file}")
else:
    print("Aucun message reçu")

consumer.close()
