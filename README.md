# Async Notification Fanout Engine

A high-throughput, asynchronous notification fanout engine built in Go. It ingests business events and reliably fans them out to multiple delivery channels (Email, SMS, Push) with strict rate limiting, idempotency, and fault tolerance.

## Architecture & Features

- **High-Throughput Ingestion API**: A REST API built with `go-chi/chi`. Ingests events in $<10\text{ms}$.
- **Strict Idempotency**: Redis `SETNX` based deduplication guarantees that duplicate events within a 24-hour window are suppressed.
- **Event Bus (Kafka)**: Ingested events are published to a KRaft-based Apache Kafka topic (`raw-events-ingest`) for asynchronous processing.
- **Audience & Preference Filtering**: PostgreSQL stores user preferences. The Fanout Worker dynamically resolves whether a user has opted out of specific channels or global communications.
- **Dynamic Templates**: The engine hydrates localized templates for each channel using Go's `html/template` package.
- **Rate-Limited Queueing (Asynq)**: Relies on Redis and `hibiken/asynq` for rate-limited, fault-tolerant queues (Token Bucket concurrency).
- **Observability**: Prometheus `/metrics` endpoint tracks throughput, success/failure rates, and latency.

## Prerequisites

- [Docker & Docker Compose](https://docs.docker.com/compose/)
- [Go 1.21+](https://go.dev/)
- [sqlc](https://sqlc.dev/) (Optional, for re-generating DB schemas)

## Local Setup & Execution

### 1. Start Infrastructure
Start PostgreSQL, Redis, and a KRaft Kafka Broker via Docker:
```bash
docker-compose up -d
```

### 2. Database Migrations
Connect to the Postgres instance (`localhost:5432`, user: `user`, password: `password`, db: `fanout_db`) and execute the schema:
```bash
psql "postgres://user:password@localhost:5432/fanout_db?sslmode=disable" -f internal/db/schema.sql
```

### 3. Run the System
The system is divided into two binaries. Run them in separate terminals:

**Start the API Server**:
```bash
go run cmd/server/main.go
```

**Start the Fanout/Delivery Worker**:
```bash
go run cmd/worker/main.go
```

### 4. Send a Test Event
```bash
curl -X POST http://localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -d '{
        "idempotency_key": "test_order_88219",
        "event_type": "ORDER_PLACED",
        "tenant_id": "tenant_123",
        "recipients": {
          "type": "DIRECT",
          "user_ids": ["usr_1"]
        },
        "payload": {
          "customer_name": "Alice"
        }
      }'
```

### 5. Run End-to-End Tests
Verify the idempotency suppression and provider metrics by running the E2E verification script:
```bash
go run cmd/test/main.go
```

You can view the Prometheus metrics at `http://localhost:8080/metrics`.

## Provider Configuration
By default, the engine falls back to `DRY-RUN` logging if API keys are missing. To connect real SDKs, supply the following environment variables to the worker process:
- `SENDGRID_API_KEY`
- `TWILIO_API_KEY`
- `FCM_API_KEY`
