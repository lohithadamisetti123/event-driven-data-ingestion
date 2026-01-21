from src.consumer import process_event


def test_process_event_transformation():
    event = {
        "event_id": "123",
        "event_type": "user_registered",
        "timestamp": "2024-01-01T00:00:00Z",
        "data": {"value_to_process": 42, "foo": "bar"},
    }

    processed = process_event(event)

    assert processed["original_event_id"] == "123"
    assert processed["event_type"] == "user_registered"
    assert "data_keys" in processed
    assert set(processed["data_keys"]) == {"value_to_process", "foo"}
    assert processed["processed_value_example"] == 42
    assert "processing_timestamp" in processed
