from datetime import datetime

from fastapi.testclient import TestClient

from src.main import app

client = TestClient(app)


def test_ingest_event_validation_error():
    resp = client.post("/api/events", json={"foo": "bar"})
    assert resp.status_code == 422


def test_health_endpoint():
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert "status" in body
    assert "kafka_connected" in body
