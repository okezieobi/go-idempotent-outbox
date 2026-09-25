package usecase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/okezieobi/go-idempotent-outbox/internal/entities"
)

var ErrLostLease = errors.New("optimistic concurrency violation: worker lost job lease")

type PostgresProcessRepo struct {
	db *sql.DB
}

func NewPostgresProcessRepo(db *sql.DB) *PostgresProcessRepo {
	return &PostgresProcessRepo{db: db}
}

func (r *PostgresProcessRepo) FetchAndLock(ctx context.Context, batchSize int, workerID string, leaseDuration time.Duration) ([]*entities.OutboxEvent, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := `
		UPDATE outbox_events
		SET status = 'processing',
		    locked_by = $1,
		    locked_until = NOW() + $2::interval,
		    updated_at = NOW()
		WHERE id IN (
			SELECT id FROM outbox_events
			WHERE (status IN ('pending', 'failed') AND next_retry_at <= NOW())
			   OR (status = 'processing' AND locked_until < NOW())
			ORDER BY created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		)
		RETURNING id, event_id, event_type, payload, retry_count, max_retries;
	`

	intervalStr := fmt.Sprintf("%d seconds", int(leaseDuration.Seconds()))

	rows, err := tx.QueryContext(ctx, query, workerID, intervalStr, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch and lock outbox events: %w", err)
	}
	defer rows.Close()

	var events []*entities.OutboxEvent
	for rows.Next() {
		e := &entities.OutboxEvent{}
		if err := rows.Scan(&e.ID, &e.EventID, &e.EventType, &e.Payload, &e.RetryCount, &e.MaxRetries); err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, e)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit fetch transaction: %w", err)
	}

	return events, nil
}

func (r *PostgresProcessRepo) MarkComplete(ctx context.Context, id string, workerID string) error {
	query := `
		UPDATE outbox_events
		SET status = 'completed',
		    locked_by = NULL,
		    locked_until = NULL,
		    updated_at = NOW()
		WHERE id = $1 AND status = 'processing' AND locked_by = $2;
	`
	res, err := r.db.ExecContext(ctx, query, id, workerID)
	if err != nil {
		return fmt.Errorf("failed to mark complete: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrLostLease
	}

	return nil
}

func (r *PostgresProcessRepo) MarkFailed(ctx context.Context, id string, workerID string, errMessage string, nextRetry time.Time, isTerminal bool) error {
	newStatus := "failed"

	query := `
		UPDATE outbox_events
		SET status = $1,
		    retry_count = retry_count + 1,
		    last_error = $2,
		    next_retry_at = $3,
		    locked_by = NULL,
		    locked_until = NULL,
		    updated_at = NOW()
		WHERE id = $4 AND status = 'processing' AND locked_by = $5;
	`
	res, err := r.db.ExecContext(ctx, query, newStatus, errMessage, nextRetry, id, workerID)
	if err != nil {
		return fmt.Errorf("failed to mark failed: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrLostLease
	}

	return nil
}

func (r *PostgresProcessRepo) ReclaimStuckJobs(ctx context.Context) (int64, error) {
	query := `
		UPDATE outbox_events
		SET status = 'pending',
		    locked_by = NULL,
		    locked_until = NULL,
		    updated_at = NOW()
		WHERE status = 'processing' AND locked_until < NOW();
	`
	res, err := r.db.ExecContext(ctx, query)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
