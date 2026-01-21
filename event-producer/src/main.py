# event-producer/src/main.py

import json
import os
from typing import Dict, Any

from fastapi import FastAPI, HTTPException, status
from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.schemas.events import RawEvent


def json_serializer(value: Dict[str, Any]) -> bytes:
    # Convert datetime and other non-serializable types to string
    return json.dumps(value, default=str).encode("utf-8")


app = FastAPI(
    title="Event Ingestion API",
    description="HTTP API for ingesting events and publishing to Kafka.",
    version="1.0.0",
)

producer: KafkaProducer | None = None
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-events")


def get_bootstrap_servers() -> list[str]:
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return [s.strip() for s in servers.split(",") if s.strip()]


@app.on_event("startup")
async def startup_event() -> None:
    global producer
    bootstrap_servers = get_bootstrap_servers()
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=json_serializer,
            retries=5,
            acks="all",
        )
    except Exception as exc:
        print(f"Failed to connect to Kafka on startup: {exc}")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    global producer
    if producer is not None:
        producer.flush()
        producer.close(timeout=5)
        producer = None


@app.post("/api/events", status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(event: RawEvent) -> Dict[str, Any]:
    global producer
    if producer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka producer not available",
        )

    try:
        payload = event.dict()
        future = producer.send(KAFKA_TOPIC, payload)
        future.get(timeout=10)
    except KafkaError as exc:
        print(f"Kafka error while sending event {event.event_id}: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send event to Kafka",
        ) from exc
    except Exception as exc:
        print(f"Unexpected error while sending event {event.event_id}: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send event to Kafka",
        ) from exc

    return {
        "message": "Event accepted for processing",
        "event_id": event.event_id,
    }


@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check() -> Dict[str, Any]:
    global producer
    bootstrap = bool(producer and producer.bootstrap_connected())
    # Consider overall status OK as long as app is running; expose Kafka flag separately
    return {
        "status": "ok",
        "kafka_connected": bootstrap,
    }
