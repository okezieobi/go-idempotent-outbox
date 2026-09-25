package usecase

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/okezieobi/go-idempotent-outbox/internal/entities"
)

type processUseCase interface {
	HandleFailure(ctx context.Context, event *entities.OutboxEvent, workerID string, executionErr error) error
	HandleSuccess(ctx context.Context, id string, workerID string) error
	FetchJobs(ctx context.Context, batchSize int, workerID string, leaseDuration time.Duration) ([]*entities.OutboxEvent, error)
}

type WorkerPool struct {
	useCase       processUseCase
	batchSize     int
	pollInterval  time.Duration
	leaseDuration time.Duration
	workerID      string
	stopChan      chan struct{}
}

func NewWorkerPool(useCase processUseCase, batchSize int, pollInterval time.Duration, leaseDuration time.Duration) *WorkerPool {
	return &WorkerPool{
		useCase:       useCase,
		batchSize:     batchSize,
		pollInterval:  pollInterval,
		leaseDuration: leaseDuration,
		workerID:      uuid.New().String(),
		stopChan:      make(chan struct{}),
	}
}

func (p *WorkerPool) Start(ctx context.Context) {
	log.Printf("Starting worker pool [ID: %s]...", p.workerID)
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.processBatch(ctx)
		case <-p.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (p *WorkerPool) Stop() {
	close(p.stopChan)
}

func (p *WorkerPool) processBatch(ctx context.Context) {
	events, err := p.useCase.FetchJobs(ctx, p.batchSize, p.workerID, p.leaseDuration)
	if err != nil {
		log.Printf("[%s] Error fetching outbox batch: %v", p.workerID, err)
		return
	}

	if len(events) == 0 {
		return
	}

	log.Printf("[%s] Locked %d jobs for processing", p.workerID, len(events))

	for _, event := range events {
		err := p.executeEvent(event)
		if err != nil {
			log.Printf("[%s] Execution failed for event %s: %v", p.workerID, event.EventID, err)
			if failErr := p.useCase.HandleFailure(ctx, event, p.workerID, err); failErr != nil {
				log.Printf("[%s] Failed updating failure state for %s: %v", p.workerID, event.EventID, failErr)
			}
			continue
		}

		if compErr := p.useCase.HandleSuccess(ctx, event.ID, p.workerID); compErr != nil {
			log.Printf("[%s] Failed marking event completed %s: %v", p.workerID, event.ID, compErr)
		}
	}
}

func (p *WorkerPool) executeEvent(event *entities.OutboxEvent) error {
	// Dispatch logic (e.g., calling external webhooks or publishing to a broker)
	return nil
}
