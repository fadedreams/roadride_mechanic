package kafka

import (
	"context"
	"time"

	"repair-service/domain"
	"log/slog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// OutboxProcessor processes events from the outbox collection
type OutboxProcessor struct {
	repo     domain.RepairRepository
	producer *Producer
	logger   *slog.Logger
}

// NewOutboxProcessor creates a new OutboxProcessor
func NewOutboxProcessor(repo domain.RepairRepository, producer *Producer, logger *slog.Logger) *OutboxProcessor {
	return &OutboxProcessor{
		repo:     repo,
		producer: producer,
		logger:   logger,
	}
}

// Start begins processing outbox events
func (p *OutboxProcessor) Start(ctx context.Context) error {
	_, span := otel.Tracer("repair-service").Start(ctx, "OutboxProcessorStart")
	defer span.End()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Stopping outbox processor", "app", "repair-service")
			return ctx.Err()
		case <-ticker.C:
			if err := p.processOutboxEvents(ctx); err != nil {
				p.logger.Error("Failed to process outbox events", "error", err, "app", "repair-service")
			}
		}
	}
}

// processOutboxEvents retrieves and publishes unprocessed outbox events
func (p *OutboxProcessor) processOutboxEvents(ctx context.Context) error {
	_, span := otel.Tracer("repair-service").Start(ctx, "ProcessOutboxEvents")
	defer span.End()

	events, err := p.repo.GetUnprocessedOutboxEvents(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to get unprocessed outbox events")
		return err
	}
	if len(events) == 0 {
		return nil
	}

	for _, event := range events {
		if err := p.producer.PublishOutboxEvent(ctx, event); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to publish outbox event")
			p.logger.Error("Failed to publish outbox event", "eventID", event.ID, "error", err, "app", "repair-service")
			continue
		}

		if err := p.repo.MarkOutboxEventProcessed(ctx, event.ID); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to mark outbox event as processed")
			p.logger.Error("Failed to mark outbox event as processed", "eventID", event.ID, "error", err, "app", "repair-service")
			continue
		}
		p.logger.Info("Processed outbox event", "eventID", event.ID, "app", "repair-service")
	}

	span.SetAttributes(
		attribute.Int("processedEventCount", len(events)),
	)
	return nil
}
