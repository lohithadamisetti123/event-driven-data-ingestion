CREATE DATABASE IF NOT EXISTS eventdb;
USE eventdb;

CREATE TABLE IF NOT EXISTS processed_events (
    id VARCHAR(36) PRIMARY KEY,
    original_event_id VARCHAR(36) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload_summary JSON NOT NULL,
    processed_at DATETIME NOT NULL,
    UNIQUE KEY uq_original_event (original_event_id)
);
