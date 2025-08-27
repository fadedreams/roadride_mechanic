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

// MongoRepository implements the RepairRepository interface
type MongoRepository struct {
	RepairCollection  *mongo.Collection
	CostCollection    *mongo.Collection
	MechanicCollection *mongo.Collection
}

// NewMongoRepository creates a new MongoRepository
func NewMongoRepository(client *mongo.Client) *MongoRepository {
	return &MongoRepository{
		RepairCollection:  client.Database("repairdb").Collection("repairs"),
		CostCollection:    client.Database("repairdb").Collection("repair_costs"),
		MechanicCollection: client.Database("repairdb").Collection("mechanics"),
	}
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

// GetAllRepairs retrieves all repairs for a given user
func (r *MongoRepository) GetAllRepairs(ctx context.Context, userID string) ([]*RepairModel, error) {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoGetAllRepairs")
	defer span.End()

	var repairs []*RepairModel
	cursor, err := r.RepairCollection.Find(ctx, bson.M{"userID": userID})
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
