package domain

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

// RepairCostModel represents the cost of a repair
type RepairCostModel struct {
	ID           string          `bson:"_id,omitempty" json:"id"`
	UserID       string          `bson:"userID" json:"userID"`
	RepairType   string          `bson:"repairType" json:"repairType"`
	TotalPrice   float64         `bson:"totalPrice" json:"totalPrice"`
	UserLocation *Location       `bson:"userLocation" json:"userLocation,omitempty"`
	Mechanics    []MechanicInfo `bson:"mechanics" json:"mechanics,omitempty"`
}

// Location represents a geographic coordinate
type Location struct {
	Longitude float64 `bson:"longitude" json:"longitude"`
	Latitude  float64 `bson:"latitude" json:"latitude"`
}

// MechanicModel represents a mechanic's details
type MechanicModel struct {
	ID       string   `bson:"_id,omitempty" json:"id"`
	Name     string   `bson:"name" json:"name"`
	Location Location `bson:"location" json:"location"`
}

// MechanicInfo represents a mechanic with distance from user
type MechanicInfo struct {
	ID       string   `bson:"id" json:"id"`
	Name     string   `bson:"name" json:"name"`
	Location Location `bson:"location" json:"location"`
	Distance float64  `bson:"distance" json:"distance"` // Distance in meters
}

// RepairModel represents a repair request
type RepairModel struct {
	ID         string           `bson:"_id,omitempty" json:"id"`
	UserID     string           `bson:"userID" json:"userID"`
	Status     string           `bson:"status" json:"status"`
	RepairCost *RepairCostModel `bson:"repairCost" json:"repairCost"`
}

// OutboxEvent represents an event in the outbox collection
type OutboxEvent struct {
	ID          string     `bson:"_id,omitempty" json:"id"`
	EventType   string     `bson:"event_type" json:"event_type"`
	Payload     []byte     `bson:"payload" json:"payload"`
	CreatedAt   time.Time  `bson:"created_at" json:"created_at"`
	Processed   bool       `bson:"processed" json:"processed"`
	ProcessedAt *time.Time `bson:"processed_at,omitempty" json:"processed_at,omitempty"`
}

// RepairRepository defines the data access methods for repairs
type RepairRepository interface {
	CreateRepair(ctx context.Context, repair *RepairModel) (*RepairModel, error)
	SaveRepairCost(ctx context.Context, cost *RepairCostModel) error
	GetRepairCostByID(ctx context.Context, id string) (*RepairCostModel, error)
	GetRepairByID(ctx context.Context, id string) (*RepairModel, error)
	UpdateRepair(ctx context.Context, repairID string, status string) error
	GetAllMechanics(ctx context.Context) ([]*MechanicModel, error)
	GetAllRepairs(ctx context.Context) ([]*RepairModel, error)
	WatchRepairs(ctx context.Context) (*mongo.ChangeStream, error)
	SaveOutboxEvent(ctx context.Context, session mongo.SessionContext, event *OutboxEvent) error
	GetUnprocessedOutboxEvents(ctx context.Context) ([]*OutboxEvent, error)
	MarkOutboxEventProcessed(ctx context.Context, eventID string) error
	GetMongoClient(ctx context.Context) *mongo.Client
}

// RepairService defines the business logic methods for repairs
type RepairService interface {
	CreateRepair(ctx context.Context, cost *RepairCostModel) (*RepairModel, error)
	EstimateRepairCost(ctx context.Context, repairType string, userID string, userLocation *Location) (*RepairCostModel, error)
	GetAndValidateRepairCost(ctx context.Context, costID, userID string) (*RepairCostModel, error)
	GetRepairByID(ctx context.Context, id string) (*RepairModel, error)
	UpdateRepair(ctx context.Context, repairID string, status string) error
	GetAllRepairs(ctx context.Context) ([]*RepairModel, error)
}
