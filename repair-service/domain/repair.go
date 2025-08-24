package domain

import (
	"context"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// RepairCostModel represents the cost details for a repair.
type RepairCostModel struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	UserID      string             `bson:"userID"`
	RepairType  string             `bson:"repairType"`
	TotalPrice  float64            `bson:"totalPrice"`
}


// RepairModel represents a repair request.
type RepairModel struct {
	ID         primitive.ObjectID `bson:"_id,omitempty"`
	UserID     string             `bson:"userID"`
	Status     string             `bson:"status"`
	RepairCost *RepairCostModel   `bson:"repairCost"`
}

// RepairRepository defines the data access methods for repairs.
type RepairRepository interface {
	CreateRepair(ctx context.Context, repair *RepairModel) (*RepairModel, error)
	SaveRepairCost(ctx context.Context, cost *RepairCostModel) error
	GetRepairCostByID(ctx context.Context, id string) (*RepairCostModel, error)
	GetRepairByID(ctx context.Context, id string) (*RepairModel, error)
	UpdateRepair(ctx context.Context, repairID string, status string) error
}

// RepairService defines the business logic methods for repairs.
type RepairService interface {
	CreateRepair(ctx context.Context, cost *RepairCostModel) (*RepairModel, error)
	EstimateRepairCost(ctx context.Context, repairType string, userID string) (*RepairCostModel, error)
	GetAndValidateRepairCost(ctx context.Context, costID, userID string) (*RepairCostModel, error)
	GetRepairByID(ctx context.Context, id string) (*RepairModel, error)
	UpdateRepair(ctx context.Context, repairID string, status string) error
}
