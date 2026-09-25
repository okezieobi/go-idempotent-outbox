package usecase

import (
	"context"
	"math"
	"time"

	"github.com/okezieobi/go-idempotent-outbox/internal/entities"
)

type repo interface {
	FetchAndLock(ctx context.Context, batchSize int, workerID string, leaseDuration time.Duration) ([]*entities.OutboxEvent, error)
	MarkComplete(ctx context.Context, id string, workerID string) error
	MarkFailed(ctx context.Context, id string, workerID string, errMessage string, nextRetry time.Time, isTerminal bool) error
	ReclaimStuckJobs(ctx context.Context) (int64, error)
}

type ProcessUseCase struct {
	repo repo
}

func NewProcessUseCase(repo repo) *ProcessUseCase {
	return &ProcessUseCase{repo: repo}
}

func (uc *ProcessUseCase) FetchJobs(ctx context.Context, batchSize int, workerID string, leaseDuration time.Duration) ([]*entities.OutboxEvent, error) {
	return uc.repo.FetchAndLock(ctx, batchSize, workerID, leaseDuration)
}

func (uc *ProcessUseCase) HandleSuccess(ctx context.Context, id string, workerID string) error {
	return uc.repo.MarkComplete(ctx, id, workerID)
}

func (uc *ProcessUseCase) HandleFailure(ctx context.Context, event *entities.OutboxEvent, workerID string, executionErr error) error {
	nextRetryCount := event.RetryCount + 1
	isTerminal := nextRetryCount >= event.MaxRetries

	backoffSeconds := math.Pow(2, float64(event.RetryCount)) * 5
	nextRetry := time.Now().Add(time.Duration(backoffSeconds) * time.Second)

	return uc.repo.MarkFailed(ctx, event.ID, workerID, executionErr.Error(), nextRetry, isTerminal)
}
