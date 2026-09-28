# Go Idempotent Outbox

A lightweight Go reference implementation of the Transactional Outbox pattern backed by PostgreSQL. It provides reliable, asynchronous event delivery with lease-based concurrency controls, exponential backoff retries, and automatic worker recovery.

## Overview

When executing database operations alongside external side effects (such as publishing to Kafka, calling payment webhooks, or dispatching emails), executing network calls inside an active database transaction can exhaust connection pools or lead to dual-write inconsistencies.

This library decouples transactional state from network delivery:
1. Domain events are inserted into PostgreSQL within the local database transaction.
2. An asynchronous worker pool polls for eligible events, acquires worker leases, and processes events.
3. Events are marked completed upon successful execution or scheduled for retry with backoff upon failure.

## Key Features

- **Lease-Based Locking:** Prevents concurrent workers from processing the same event.
- **Worker Crash Recovery:** Stale jobs with expired leases are automatically reclaimed via `ReclaimStuckJobs`.
- **Truncated Exponential Backoff:** Failed jobs are delayed before retrying to prevent overwhelming downstream services.
- **Optimistic Ownership Guard:** Completion updates verify worker identity (`locked_by`), preventing stale workers from completing jobs whose lease has expired.

## Project Structure

```text
.
├── docker-compose.yml
├── go.mod
├── main/
│   └── index.go                   # Entrypoint & worker pool initialization
├── internal/
│   ├── entities/
│   │   └── outbox.go              # Outbox entity definitions & status enum
│   └── usecase/
│       ├── process.repo.go        # PostgreSQL query implementations & transactions
│       ├── process.service.go     # Business execution logic & backoff calculations
│       └── process.worker.go      # Concurrent worker pool & polling loops
├── migrations/
│   └── 000001_create_outbox_table.up.sql
└── README.md
```

## Outbox Workflow

```
[ Domain Tx ] ──> INSERT INTO outbox_events (status: 'pending')
                         │
                         ▼
┌────────────────────────────────────────────────────────┐
│ Worker Pool (process.worker.go)                        │
│  1. Fetch & Lock (status: 'processing', set lease)      │
│  2. Execute Handler                                     │
│     ├─ Success ──> MarkComplete (assert locked_by)     │
│     └─ Failure ──> MarkFailed (increment retry_count) │
└────────────────────────────────────────────────────────┘
```

## Database Schema

Defined in `migrations/000001_create_outbox_table.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 5,
    last_error TEXT,
    locked_by VARCHAR(255),
    locked_until TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## Getting Started

### Prerequisites

- **Go** `1.26+`
- **Docker** and **Docker Compose**
- **PostgreSQL 16** (supplied via Compose)

### 1. Start Database Container

```bash
docker compose up -d
```

Starts PostgreSQL on `localhost:5432` with database `outbox_db`.

### 2. Run Database Migrations

Apply the migration schema using `psql`:

```bash
psql "host=localhost port=5432 user=postgres password=postgrespassword dbname=outbox_db sslmode=disable" -f migrations/000001_create_outbox_table.up.sql
```

### 3. Start the Worker Service

```bash
go run ./main
```

## Runtime Configuration

Worker parameters are configured in `main/index.go`:

| Parameter | Default | Description |
| :--- | :--- | :--- |
| **Batch Size** | `10` | Maximum number of events locked per poll cycle |
| **Poll Interval** | `2s` | Wait duration between polling iterations |
| **Lease Duration** | `30s` | Time window before a locked job is considered stuck |

## Retries & Safety Guarantees

- **Lost Lease Prevention:** If execution exceeds the `locked_until` threshold, another worker may reclaim the job. When the original worker attempts to call `MarkComplete`, the update fails because `locked_by` no longer matches, preserving data consistency.
- **Exponential Backoff:** Failures calculate the next retry window as a function of `retry_count`, insulating external endpoints during outage spikes.

## License

This project is licensed under the MIT License.