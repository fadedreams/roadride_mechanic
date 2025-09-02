package domain

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// MongoRepository implements the RepairRepository interface
type MongoRepository struct {
	RepairCollection   *mongo.Collection
	CostCollection     *mongo.Collection
	MechanicCollection *mongo.Collection
	OutboxCollection   *mongo.Collection
}

// NewMongoRepository creates a new MongoRepository
func NewMongoRepository(client *mongo.Client) *MongoRepository {
	return &MongoRepository{
		RepairCollection:   client.Database("repairdb").Collection("repairs"),
		CostCollection:     client.Database("repairdb").Collection("repair_costs"),
		MechanicCollection: client.Database("repairdb").Collection("mechanics"),
		OutboxCollection:   client.Database("repairdb").Collection("outbox"),
	}
}

// GetMongoClient returns the MongoDB client for starting sessions
func (r *MongoRepository) GetMongoClient(ctx context.Context) *mongo.Client {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoGetMongoClient")
	defer span.End()
	return r.RepairCollection.Database().Client()
}

// CreateRepair inserts a new repair
func (r *MongoRepository) CreateRepair(ctx context.Context, repair *RepairModel) (*RepairModel, error) {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoCreateRepair")
	defer span.End()

	_, err := r.RepairCollection.InsertOne(ctx, repair)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to insert repair")
		return nil, err
	}
	span.SetAttributes(
		attribute.String("repairID", repair.ID),
		attribute.String("userID", repair.UserID),
		attribute.String("status", repair.Status),
	)
	return repair, nil
}

// SaveRepairCost inserts a new repair cost
func (r *MongoRepository) SaveRepairCost(ctx context.Context, cost *RepairCostModel) error {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoSaveRepairCost")
	defer span.End()

	_, err := r.CostCollection.InsertOne(ctx, cost)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to insert repair cost")
		return err
	}
	span.SetAttributes(
		attribute.String("costID", cost.ID),
		attribute.String("userID", cost.UserID),
		attribute.String("repairType", cost.RepairType),
		attribute.Float64("totalPrice", cost.TotalPrice),
	)
	return nil
}

// GetRepairCostByID retrieves a repair cost by ID
func (r *MongoRepository) GetRepairCostByID(ctx context.Context, id string) (*RepairCostModel, error) {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoGetRepairCostByID")
	defer span.End()

	var cost RepairCostModel
	err := r.CostCollection.FindOne(ctx, bson.M{"_id": id}).Decode(&cost)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find repair cost")
		return nil, err
	}
	span.SetAttributes(
		attribute.String("costID", id),
		attribute.String("userID", cost.UserID),
	)
	return &cost, nil
}

// GetRepairByID retrieves a repair by ID
func (r *MongoRepository) GetRepairByID(ctx context.Context, id string) (*RepairModel, error) {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoGetRepairByID")
	defer span.End()

	var repair RepairModel
	err := r.RepairCollection.FindOne(ctx, bson.M{"_id": id}).Decode(&repair)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find repair")
		return nil, err
	}
	span.SetAttributes(
		attribute.String("repairID", id),
		attribute.String("userID", repair.UserID),
	)
	return &repair, nil
}

// UpdateRepair updates the status of a repair
func (r *MongoRepository) UpdateRepair(ctx context.Context, repairID string, status string) error {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoUpdateRepair")
	defer span.End()

	_, err := r.RepairCollection.UpdateOne(ctx, bson.M{"_id": repairID}, bson.M{"$set": bson.M{"status": status}})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to update repair")
		return err
	}
	span.SetAttributes(
		attribute.String("repairID", repairID),
		attribute.String("status", status),
	)
	return nil
}

// GetAllMechanics retrieves all mechanics
func (r *MongoRepository) GetAllMechanics(ctx context.Context) ([]*MechanicModel, error) {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoGetAllMechanics")
	defer span.End()

	var mechanics []*MechanicModel
	cursor, err := r.MechanicCollection.Find(ctx, bson.M{})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find mechanics")
		return nil, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var mechanic MechanicModel
		if err := cursor.Decode(&mechanic); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to decode mechanic")
			return nil, err
		}
		mechanics = append(mechanics, &mechanic)
	}
	if err := cursor.Err(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Cursor error")
		return nil, err
	}
	span.SetAttributes(
		attribute.Int("mechanicCount", len(mechanics)),
	)
	return mechanics, nil
}

// GetAllRepairs retrieves all repairs
func (r *MongoRepository) GetAllRepairs(ctx context.Context) ([]*RepairModel, error) {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoGetAllRepairs")
	defer span.End()

	var repairs []*RepairModel
	cursor, err := r.RepairCollection.Find(ctx, bson.M{})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find repairs")
		return nil, fmt.Errorf("failed to find repairs: %v", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var repair RepairModel
		if err := cursor.Decode(&repair); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to decode repair")
			return nil, fmt.Errorf("failed to decode repair: %v", err)
		}
		repairs = append(repairs, &repair)
	}
	if err := cursor.Err(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Cursor error")
		return nil, fmt.Errorf("cursor error: %v", err)
	}

	span.SetAttributes(
		attribute.Int("repairCount", len(repairs)),
	)

	return repairs, nil
}

// WatchRepairs sets up a MongoDB change stream for repair insertions
func (r *MongoRepository) WatchRepairs(ctx context.Context) (*mongo.ChangeStream, error) {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoWatchRepairs")
	defer span.End()

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{{Key: "operationType", Value: "insert"}}}},
	}
	changeStream, err := r.RepairCollection.Watch(ctx, pipeline, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to open change stream")
		return nil, fmt.Errorf("failed to open change stream: %v", err)
	}
	return changeStream, nil
}

// SaveOutboxEvent saves an event to the outbox collection
func (r *MongoRepository) SaveOutboxEvent(ctx context.Context, session mongo.SessionContext, event *OutboxEvent) error {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoSaveOutboxEvent")
	defer span.End()

	_, err := r.OutboxCollection.InsertOne(session, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to save outbox event")
		return err
	}
	span.SetAttributes(
		attribute.String("eventID", event.ID),
		attribute.String("eventType", event.EventType),
	)
	return nil
}

// GetUnprocessedOutboxEvents retrieves unprocessed outbox events
func (r *MongoRepository) GetUnprocessedOutboxEvents(ctx context.Context) ([]*OutboxEvent, error) {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoGetUnprocessedOutboxEvents")
	defer span.End()

	var events []*OutboxEvent
	cursor, err := r.OutboxCollection.Find(ctx, bson.M{"processed": false})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find unprocessed outbox events")
		return nil, fmt.Errorf("failed to find unprocessed outbox events: %v", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var event OutboxEvent
		if err := cursor.Decode(&event); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to decode outbox event")
			return nil, fmt.Errorf("failed to decode outbox event: %v", err)
		}
		events = append(events, &event)
	}
	if err := cursor.Err(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Cursor error")
		return nil, fmt.Errorf("cursor error: %v", err)
	}

	span.SetAttributes(
		attribute.Int("eventCount", len(events)),
	)
	return events, nil
}

// MarkOutboxEventProcessed marks an outbox event as processed
func (r *MongoRepository) MarkOutboxEventProcessed(ctx context.Context, eventID string) error {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoMarkOutboxEventProcessed")
	defer span.End()

	now := time.Now()
	_, err := r.OutboxCollection.UpdateOne(ctx, bson.M{"_id": eventID}, bson.M{
		"$set": bson.M{
			"processed":    true,
			"processed_at": now,
		},
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to mark outbox event as processed")
		return err
	}
	span.SetAttributes(
		attribute.String("eventID", eventID),
	)
	return nil
}
