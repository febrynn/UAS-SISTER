import json
import threading
from broker.redis_client import redis_client
from api.worker.processor.db import insert_event

CHANNEL_NAME = "events"


def consume_events():
    pubsub = redis_client.pubsub()
    pubsub.subscribe(CHANNEL_NAME)

    print("📡 Worker listening to Redis channel:", CHANNEL_NAME)

    for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            event = json.loads(message["data"])
            insert_event(
                topic=event["topic"],
                event_id=event["event_id"]
            )
            print("✅ Event processed:", event["event_id"])

        except Exception as e:
            print("❌ Failed to process event:", e)


def start_worker():
    thread = threading.Thread(target=consume_events, daemon=True)
    thread.start()
