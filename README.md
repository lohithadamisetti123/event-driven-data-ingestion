# Event-Driven Data Ingestion Service

Event-driven backend using **FastAPI**, **Apache Kafka**, and **MySQL**. It ingests events via HTTP, publishes them to Kafka, processes them asynchronously in a consumer service, and stores processed results in MySQL.




---

## Architecture

- **Event Producer (FastAPI)**  
  - `POST /api/events` validates JSON payloads and publishes to Kafka topic `raw-events`.  
  - Returns `202 Accepted` immediately (no blocking on processing).

- **Kafka + Zookeeper**  
  - Kafka acts as the message broker.  
  - Single topic: `raw-events`.

- **Event Consumer (Python worker)**  
  - Subscribes to `raw-events`.  
  - Transforms the event (extracts fields, adds `processing_timestamp`).  
  - Inserts into MySQL table `processed_events`.

- **MySQL**  
  - Table `processed_events` with columns:  
    - `id` (UUID, PK)  
    - `original_event_id` (UUID, unique)  
    - `event_type` (string)  
    - `payload_summary` (JSON)  
    - `processed_at` (DATETIME)  
  - Unique `original_event_id` makes processing **idempotent**.

All services are orchestrated with **Docker Compose** and can be started with a single command.

---

## Quick Start

### 1. Environment

```bash
cp .env.example .env
```

`.env.example` defines:

- MySQL: `MYSQL_ROOT_PASSWORD`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD`  
- Kafka: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`  
- Producer: `PRODUCER_API_PORT`
  
### 2. Run stack

```bash
docker-compose up --build
docker-compose ps   # check all services are Up / healthy
```

---

## API

### Health

```bash
curl http://localhost:8000/health
```

Example:

```json
{
  "status": "ok",
  "kafka_connected": false
}
```

### Ingest event

```bash
curl -X POST http://localhost:8000/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "test_event",
    "timestamp": "2026-01-21T15:30:00Z",
    "data": { "value_to_process": 777, "foo": "bar" }
  }'
```

Response:

```json
{
  "message": "Event accepted for processing",
  "event_id": "..."
}
```

Swagger docs:

- http://localhost:8000/docs

---

## Check processed events

```bash
docker-compose exec mysql mysql -h localhost \
  -ueventuser -peventpassword eventdb \
  -e "SELECT id, original_event_id, event_type, processed_at FROM processed_events ORDER BY processed_at DESC LIMIT 5;"
```

You should see the `test_event` with matching `original_event_id` and a recent `processed_at`.

---

## Tests

Run all tests inside Docker:

```bash
# Producer unit tests
docker-compose run --rm event-producer pytest -q tests

# Consumer unit tests
docker-compose run --rm event-consumer pytest -q tests

# End-to-end integration tests
docker-compose run --rm integration-tests
```

---

## Repo

```text
https://github.com/lohithadamisetti123/event-driven-data-ingestion


