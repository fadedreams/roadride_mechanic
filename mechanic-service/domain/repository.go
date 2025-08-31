// mechanic-service/domain/repository.go
package domain

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type MechanicRepository interface{
	GetMechanicByID(ctx context.Context, id string) (*Mechanic, error)
	GetAllRepairs(ctx context.Context) ([]*Repair, error)
	AssignRepair(ctx context.Context, repairID, mechanicID string) (*Repair, error)
}
// MongoRepository implements the MechanicRepository interface
type MongoRepository struct {
	MechanicCollection *mongo.Collection
	RepairCollection   *mongo.Collection
}

// NewMongoRepository creates a new MongoRepository
func NewMongoRepository(client *mongo.Client) *MongoRepository {
	return &MongoRepository{
		MechanicCollection: client.Database("repairdb").Collection("mechanics"),
		RepairCollection:   client.Database("repairdb").Collection("repairs"),
	}
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
		chSpan := otel.Tracer("mechanic-service").Start(ctx, "MongoDecodeRepair")
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
