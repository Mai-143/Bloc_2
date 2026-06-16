import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime

KAFKA_TOPIC = "github_events"
KAFKA_SERVER = "kafka:29092"
GITHUB_EVENTS_URL = "https://api.github.com/events"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_github_events():
    response = requests.get(GITHUB_EVENTS_URL, timeout=30)
    response.raise_for_status()
    return response.json()

def normalize_event(event):
    repo = event.get("repo", {})
    actor = event.get("actor", {})

    return {
        "event_id": event.get("id"),
        "event_type": event.get("type"),
        "repo_name": repo.get("name"),
        "actor_login": actor.get("login"),
        "created_at": event.get("created_at"),
        "collected_at": datetime.utcnow().isoformat()
    }

def main():
    print("GitHub Events Producer démarré...")

    while True:
        events = fetch_github_events()

        for event in events:
            clean_event = normalize_event(event)
            producer.send(KAFKA_TOPIC, clean_event)
            print(f"Envoyé : {clean_event['event_type']} - {clean_event['repo_name']}")

        producer.flush()
        time.sleep(30)

if __name__ == "__main__":
    main()