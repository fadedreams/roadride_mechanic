package domain

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// MechanicRepository defines the data access methods for mechanics
type MechanicRepository interface {
	GetMechanicByID(ctx context.Context, id string) (*Mechanic, error)
	GetAllRepairs(ctx context.Context) ([]*Repair, error)
	AssignRepair(ctx context.Context, repairID, mechanicID string) (*Repair, error)
	SaveOutboxEvent(ctx context.Context, session mongo.SessionContext, event *OutboxEvent) error
	GetUnprocessedOutboxEvents(ctx context.Context) ([]*OutboxEvent, error)
	MarkOutboxEventProcessed(ctx context.Context, eventID string) error
	InsertRepair(ctx context.Context, session mongo.SessionContext, repair *Repair) error
	GetMongoClient(ctx context.Context) *mongo.Client
	CheckRepairExists(ctx context.Context, session mongo.SessionContext, repairID string) (bool, error)
}

// MongoRepository implements the MechanicRepository interface
type MongoRepository struct {
	MechanicCollection *mongo.Collection
	RepairCollection   *mongo.Collection
	OutboxCollection   *mongo.Collection
	client             *mongo.Client
}

// NewMongoRepository creates a new MongoRepository
func NewMongoRepository(client *mongo.Client) *MongoRepository {
	return &MongoRepository{
		MechanicCollection: client.Database("repairdb").Collection("mechanics"),
		RepairCollection:   client.Database("repairdb").Collection("repairs"),
		OutboxCollection:   client.Database("repairdb").Collection("mechanic_outbox"),
		client:             client,
	}
}

// GetMongoClient returns the MongoDB client
func (r *MongoRepository) GetMongoClient(ctx context.Context) *mongo.Client {
	return r.client
}

// GetMechanicByID retrieves a mechanic by ID
func (r *MongoRepository) GetMechanicByID(ctx context.Context, id string) (*Mechanic, error) {
	_, span := otel.Tracer("mechanic-service").Start(ctx, "MongoGetMechanicByID")
	defer span.End()

	var mechanic Mechanic
	err := r.MechanicCollection.FindOne(ctx, bson.M{"_id": id}).Decode(&mechanic)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find mechanic")
		return nil, fmt.Errorf("failed to find mechanic: %v", err)
	}
	span.SetAttributes(
		attribute.String("mechanicID", id),
		attribute.String("mechanicName", mechanic.Name),
	)
	return &mechanic, nil
}

// GetAllRepairs retrieves all repairs
func (r *MongoRepository) GetAllRepairs(ctx context.Context) ([]*Repair, error) {
	_, span := otel.Tracer("mechanic-service").Start(ctx, "MongoGetAllRepairs")
	defer span.End()

	var repairs []*Repair
	cursor, err := r.RepairCollection.Find(ctx, bson.M{})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find repairs")
		return nil, fmt.Errorf("failed to find repairs: %v", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		_, chSpan := otel.Tracer("mechanic-service").Start(ctx, "MongoDecodeRepair")
		var repair Repair
		if err := cursor.Decode(&repair); err != nil {
			chSpan.RecordError(err)
			chSpan.SetStatus(codes.Error, "Failed to decode repair")
			chSpan.End()
			return nil, fmt.Errorf("failed to decode repair: %v", err)
		}
		chSpan.End()
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

// AssignRepair assigns a mechanic to a repair
func (r *MongoRepository) AssignRepair(ctx context.Context, repairID, mechanicID string) (*Repair, error) {
	_, span := otel.Tracer("mechanic-service").Start(ctx, "MongoAssignRepair")
	defer span.End()

	var repair Repair
	if err := r.RepairCollection.FindOne(ctx, bson.M{"_id": repairID}).Decode(&repair); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find repair")
		return nil, fmt.Errorf("failed to find repair: %v", err)
	}

	update := bson.M{"$set": bson.M{"assignedTo": mechanicID}}
	if _, err := r.RepairCollection.UpdateOne(ctx, bson.M{"_id": repairID}, update); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to assign repair")
		return nil, fmt.Errorf("failed to assign repair: %v", err)
	}

	repair.AssignedTo = mechanicID
	span.SetAttributes(
		attribute.String("repairID", repairID),
		attribute.String("mechanicID", mechanicID),
	)
	return &repair, nil
}

// SaveOutboxEvent saves an event to the outbox collection
func (r *MongoRepository) SaveOutboxEvent(ctx context.Context, session mongo.SessionContext, event *OutboxEvent) error {
	_, span := otel.Tracer("mechanic-service").Start(ctx, "MongoSaveOutboxEvent")
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
	_, span := otel.Tracer("mechanic-service").Start(ctx, "MongoGetUnprocessedOutboxEvents")
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
	_, span := otel.Tracer("mechanic-service").Start(ctx, "MongoMarkOutboxEventProcessed")
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

// InsertRepair inserts a repair into the repairs collection
func (r *MongoRepository) InsertRepair(ctx context.Context, session mongo.SessionContext, repair *Repair) error {
	_, span := otel.Tracer("mechanic-service").Start(ctx, "MongoInsertRepair")
	defer span.End()

	_, err := r.RepairCollection.InsertOne(session, repair)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to insert repair")
		return err
	}
	span.SetAttributes(
		attribute.String("repairID", repair.ID),
		attribute.String("userID", repair.UserID),
		attribute.String("status", repair.Status),
	)
	return nil
}

// CheckRepairExists checks if a repair exists by ID
func (r *MongoRepository) CheckRepairExists(ctx context.Context, session mongo.SessionContext, repairID string) (bool, error) {
	_, span := otel.Tracer("mechanic-service").Start(ctx, "MongoCheckRepairExists")
	defer span.End()

	var repair Repair
	err := r.RepairCollection.FindOne(session, bson.M{"_id": repairID}).Decode(&repair)
	if err == mongo.ErrNoDocuments {
		return false, nil
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to check repair existence")
		return false, fmt.Errorf("failed to check repair existence: %v", err)
	}
	span.SetAttributes(
		attribute.String("repairID", repairID),
		attribute.Bool("exists", true),
	)
	return true, nil
}
