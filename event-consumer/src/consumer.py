import json
import os
import time
from datetime import datetime
from typing import Any, Dict
from uuid import uuid4

import mysql.connector
from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "event-processor-group")
KAFKA_BOOTSTRAP_SERVERS = [
    s.strip()
    for s in os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    if s.strip()
]

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "eventdb")
MYSQL_USER = os.getenv("MYSQL_USER", "eventuser")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "eventpassword")


def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        autocommit=False,
    )


def process_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
    processed_summary: Dict[str, Any] = {
        "original_event_id": event_data.get("event_id"),
        "event_type": event_data.get("event_type"),
        "data_keys": list(event_data.get("data", {}).keys()),
        "processed_value_example": event_data.get("data", {}).get(
            "value_to_process", "N/A"
        ),
        "processing_timestamp": datetime.utcnow().isoformat(),
    }
    return processed_summary


def store_processed_event(db_connection, processed_event_data: Dict[str, Any]) -> None:
    cursor = db_connection.cursor()
    insert_query = (
        "INSERT INTO processed_events (id, original_event_id, event_type, "
        "payload_summary, processed_at) VALUES (%s, %s, %s, %s, %s)"
    )
    event_id = str(uuid4())
    original_event_id = processed_event_data["original_event_id"]
    event_type = processed_event_data["event_type"]
    payload_summary_json = json.dumps(processed_event_data)
    processed_at = processed_event_data["processing_timestamp"]

    # Simple idempotency: rely on unique constraint on original_event_id
    try:
        cursor.execute(
            insert_query,
            (event_id, original_event_id, event_type, payload_summary_json, processed_at),
        )
        db_connection.commit()
        print(f"Stored processed event {event_id} for original ID {original_event_id}")
    except mysql.connector.errors.IntegrityError:
        db_connection.rollback()
        print(
            f"Duplicate event detected for original ID {original_event_id}, skipping (idempotent)."
        )
    finally:
        cursor.close()


def run_consumer() -> None:
    consumer = None
    db_connection = None
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        print(
            f"Kafka Consumer started for topic '{KAFKA_TOPIC}' in group '{KAFKA_GROUP_ID}'"
        )

        db_connection = get_db_connection()
        print("Database connection established.")

        for message in consumer:
            try:
                event_data = message.value
                print(
                    f"Received event: {event_data.get('event_id')}, type: {event_data.get('event_type')}"
                )

                processed_event = process_event(event_data)
                store_processed_event(db_connection, processed_event)

                consumer.commit()
                print(
                    f"Event {event_data.get('event_id')} processed and offset committed."
                )
            except mysql.connector.Error as db_err:
                print(
                    f"Database error processing event {message.value.get('event_id')}: {db_err}"
                )
                db_connection.rollback()
            except Exception as exc:
                print(
                    f"Error processing event {message.value.get('event_id')}: {exc}"
                )

    except KafkaError as exc:
        print(f"Consumer encountered a Kafka error: {exc}")
    except Exception as exc:
        print(f"Consumer encountered a critical error: {exc}")
    finally:
        if consumer is not None:
            consumer.close()
            print("Kafka Consumer closed.")
        if db_connection is not None:
            db_connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    time.sleep(30)
    run_consumer()
