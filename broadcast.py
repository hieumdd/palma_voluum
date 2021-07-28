import os
import json

from google.cloud import pubsub_v1

def get_tables():
    path = 'configs/'
    tables = [i.replace('.json', '') for i in os.listdir(path)]
    return tables

def broadcast(broadcast_data):
    tables = get_tables()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))

    for table in tables:
        data = {
            "table": table,
            "start": broadcast_data.get("start"),
            "end": broadcast_data.get("end"),
        }
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()

    return tables
