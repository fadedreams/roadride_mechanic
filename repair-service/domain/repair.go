package domain

import (
	"context"
)

type RepairCostModel struct {
    ID          string  `bson:"_id,omitempty" json:"id"`
    UserID      string  `bson:"userID" json:"userID"`
    RepairType  string  `bson:"repairType" json:"repairType"`
    TotalPrice  float64 `bson:"totalPrice" json:"totalPrice"`
}

type RepairModel struct {
    ID         string           `bson:"_id,omitempty" json:"id"`
    UserID     string           `bson:"userID" json:"userID"`
    Status     string           `bson:"status" json:"status"`
    RepairCost *RepairCostModel `bson:"repairCost" json:"repairCost"`
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
