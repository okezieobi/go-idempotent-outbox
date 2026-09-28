# Go Idempotent Outbox

A small Go example that demonstrates the outbox pattern using PostgreSQL and a background worker pool. The project focuses on reliably publishing domain events by recording them in an outbox table and processing them asynchronously with retry, lease-based locking, and idempotent completion handling.

## Overview

The project is structured around a PostgreSQL-backed `outbox_events` table and a worker that polls for pending work, locks it, and executes the event handler. The design is intentionally simple and suitable as a reference implementation for a production-grade outbox processor.

Key characteristics:

- Outbox events stored in PostgreSQL
- Worker pool for batched processing
- Lease-based locking to avoid duplicate processing
- Retry with exponential backoff
- Failed jobs can be reclaimed when a lease expires
- Status tracking: `pending`, `processing`, `completed`, `failed`

## Project structure

```text
.
├── docker-compose.yml
├── go.mod
├── main/
│   └── index.go
├── internal/
│   ├── entities/
│   │   └── outbox.go
│   └── usecase/
│       ├── process.repo.go
│       ├── process.service.go
│       └── process.worker.go
├── migrations/
│   └── 000001_create_outbox_table.up.sql
└── README.md
```

## Outbox workflow

1. An application stores an event in the `outbox_events` table as `pending`.
2. A worker fetches a batch of eligible events.
3. The worker updates the row to `processing` and sets a lock/lease using `locked_by` and `locked_until`.
4. The event is executed.
5. On success, the row is marked `completed`.
6. On failure, the row is marked `failed` with a retry timestamp and exponential backoff delay.
7. If a worker crashes or loses the lease, the row can be reclaimed and retried.

## Database schema

The schema is defined in:

- [migrations/000001_create_outbox_table.up.sql](migrations/000001_create_outbox_table.up.sql)

It creates a table with these important fields:

- `id`
- `event_id`
- `event_type`
- `payload`
- `status`
- `retry_count`
- `max_retries`
- `last_error`
- `locked_by`
- `locked_until`
- `next_retry_at`
- `created_at`
- `updated_at`

## Prerequisites

- Go 1.26+
- Docker and Docker Compose
- PostgreSQL 16 (provided by the included compose file)

## Getting started

### 1. Start PostgreSQL

From the project root:

```bash
docker compose up -d
```

This starts a PostgreSQL instance on:

- Host: `localhost`
- Port: `5432`
- Database: `outbox_db`
- User: `postgres`
- Password: `postgrespassword`

### 2. Apply the schema

The project includes the migration SQL in [migrations/000001_create_outbox_table.up.sql](migrations/000001_create_outbox_table.up.sql). You can load it manually with psql:

```bash
psql "host=localhost port=5432 user=postgres password=postgrespassword dbname=outbox_db sslmode=disable" -f migrations/000001_create_outbox_table.up.sql
```

### 3. Run the app

```bash
go run ./main
```

The app initializes the database connection, creates the repository, spins up a worker pool, and starts polling for outbox jobs.

## Runtime configuration

The application currently configures the worker pool in [main/index.go](main/index.go):

- Batch size: `10`
- Poll interval: `2s`
- Lease duration: `30s`

The connection string is also hardcoded in [main/index.go](main/index.go) to match the local Docker Postgres instance.

## How the worker behaves

The worker pool is implemented in [internal/usecase/process.worker.go](internal/usecase/process.worker.go):

- It periodically fetches work from PostgreSQL.
- It locks the work using a lease to avoid duplicate processing.
- It executes each event through `executeEvent`.
- It marks the event as completed or failed depending on the result.

At the moment, `executeEvent` is a placeholder and returns `nil`. In a real implementation, this is where you would publish to Kafka, RabbitMQ, an HTTP webhook, or another downstream system.

## Retry and locking model

The repository logic in [internal/usecase/process.repo.go](internal/usecase/process.repo.go) handles the critical parts of the pattern:

- `FetchAndLock`: selects pending or failed jobs due for retry and marks them as processing
- `MarkComplete`: confirms completion only if the worker still owns the lease
- `MarkFailed`: updates status, increments retries, and schedules a next retry time
- `ReclaimStuckJobs`: resets any job whose lease has expired

This protects the system from:

- duplicate deliveries
- worker crashes during processing
- stale lease problems

## Notes

This repository is best viewed as a reference implementation rather than a full production messaging platform. It intentionally keeps the code small and understandable while demonstrating the core mechanics of an idempotent outbox processor.

If you want to extend it, the next logical steps are:

- add a real event dispatcher
- support multiple event types with typed handlers
- add an API or command layer to insert outbox records
- integrate with Kafka, NATS, or another broker
- add tests around retries and lease recovery

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
