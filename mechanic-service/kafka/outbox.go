package kafka

import (
	"context"
	"fmt"
	"time"

	"log/slog"
	"mechanic-service/domain"

	"github.com/hamba/avro/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// OutboxProcessor processes events from the outbox collection
type OutboxProcessor struct {
	repo   domain.MechanicRepository
	logger *slog.Logger
	schema avro.Schema
}

// NewOutboxProcessor creates a new OutboxProcessor
func NewOutboxProcessor(repo domain.MechanicRepository, logger *slog.Logger, schema avro.Schema) *OutboxProcessor {
	return &OutboxProcessor{
		repo:   repo,
		logger: logger,
		schema: schema,
	}
}

// Start begins processing outbox events
func (p *OutboxProcessor) Start(ctx context.Context) error {
	_, span := otel.Tracer("mechanic-service").Start(ctx, "OutboxProcessorStart")
	defer span.End()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Stopping outbox processor", "app", "mechanic-service")
			return ctx.Err()
		case <-ticker.C:
			if err := p.processOutboxEvents(ctx); err != nil {
				p.logger.Error("Failed to process outbox events", "error", err, "app", "mechanic-service")
			}
		}
	}
}

// processOutboxEvents retrieves and processes unprocessed outbox events
func (p *OutboxProcessor) processOutboxEvents(ctx context.Context) error {
	_, span := otel.Tracer("mechanic-service").Start(ctx, "ProcessOutboxEvents")
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
		// Deserialize the event payload
		var repairEvent RepairEvent
		if len(event.Payload) < 5 {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Invalid payload length")
			p.logger.Error("Invalid payload length", "eventID", event.ID, "length", len(event.Payload), "app", "mechanic-service")
			continue
		}
		err := avro.Unmarshal(p.schema, event.Payload[5:], &repairEvent)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to deserialize event")
			p.logger.Error("Failed to deserialize event", "eventID", event.ID, "error", err, "app", "mechanic-service")
			continue
		}

		// Convert RepairEvent to domain.Repair
		var userLocation *domain.Location
		if repairEvent.UserLocation != nil {
			userLocation = &domain.Location{
				Longitude: repairEvent.UserLocation.Longitude,
				Latitude:  repairEvent.UserLocation.Latitude,
			}
		}
		mechanics := make([]domain.MechanicInfo, len(repairEvent.Mechanics))
		for i, m := range repairEvent.Mechanics {
			mechanics[i] = domain.MechanicInfo{
				ID:       m.ID,
				Name:     m.Name,
				Location: domain.Location{
					Longitude: m.Location.Longitude,
					Latitude:  m.Location.Latitude,
				},
				Distance: m.Distance,
			}
		}
		repair := &domain.Repair{
			ID:     repairEvent.ID,
			UserID: repairEvent.UserID,
			Status: repairEvent.Status,
			RepairCost: &domain.RepairCost{
				ID:           repairEvent.ID, // Assuming same ID for simplicity
				UserID:       repairEvent.UserID,
				RepairType:   repairEvent.RepairType,
				TotalPrice:   repairEvent.TotalPrice,
				UserLocation: userLocation,
				Mechanics:    mechanics,
			},
		}

		// Start a transaction to check and insert repair
		session, err := p.repo.GetMongoClient(ctx).StartSession()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to start MongoDB session")
			p.logger.Error("Failed to start MongoDB session", "eventID", event.ID, "error", err, "app", "mechanic-service")
			continue
		}
		defer session.EndSession(ctx)

		err = session.StartTransaction()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to start transaction")
			p.logger.Error("Failed to start transaction", "eventID", event.ID, "error", err, "app", "mechanic-service")
			continue
		}

		err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
			// Check if repair already exists
			var existing Repair
			err := p.repo.(*MongoRepository).RepairCollection.FindOne(sc, bson.M{"_id": repair.ID}).Decode(&existing)
			if err == nil {
				p.logger.Info("Repair already exists, skipping", "repairID", repair.ID, "app", "mechanic-service")
				return nil
			}
			if err != mongo.ErrNoDocuments {
				return fmt.Errorf("failed to check existing repair: %w", err)
			}

			// Insert the repair
			if err := p.repo.InsertRepair(sc, repair); err != nil {
				return fmt.Errorf("failed to insert repair: %w", err)
			}
			p.logger.Info("Inserted repair in transaction", "repairID", repair.ID, "app", "mechanic-service")

			// Mark the outbox event as processed
			if err := p.repo.MarkOutboxEventProcessed(sc, event.ID); err != nil {
				return fmt.Errorf("failed to mark outbox event as processed: %w", err)
			}
			p.logger.Info("Marked outbox event as processed in transaction", "eventID", event.ID, "app", "mechanic-service")

			return nil
		})
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Transaction failed")
			p.logger.Error("Transaction failed", "eventID", event.ID, "error", err, "app", "mechanic-service")
			session.AbortTransaction(ctx)
			continue
		}

		if err := session.CommitTransaction(ctx); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to commit transaction")
			p.logger.Error("Failed to commit transaction", "eventID", event.ID, "error", err, "app", "mechanic-service")
			continue
		}

		p.logger.Info("Committed transaction for outbox event", "eventID", event.ID, "repairID", repair.ID, "app", "mechanic-service")
	}

	span.SetAttributes(
		attribute.Int("processedEventCount", len(events)),
	)
	return nil
}
