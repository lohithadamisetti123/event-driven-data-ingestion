import os
import time
from datetime import datetime
from uuid import uuid4

import mysql.connector
import pytest
import requests


PRODUCER_API_URL = os.getenv("PRODUCER_API_URL", "http://localhost:8000")
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
    )


@pytest.mark.integration
def test_event_flow_end_to_end():
    original_event_id = str(uuid4())
    payload = {
        "event_id": original_event_id,
        "event_type": "test_event",
        "timestamp": datetime.utcnow().isoformat(),
        "data": {"value_to_process": 123, "foo": "bar"},
    }

    resp = requests.post(f"{PRODUCER_API_URL}/api/events", json=payload)
    assert resp.status_code == 202
    body = resp.json()
    assert body["event_id"] == original_event_id

    time.sleep(20)

    conn = get_db_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM processed_events WHERE original_event_id = %s",
            (original_event_id,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["event_type"] == "test_event"
    finally:
        conn.close()
