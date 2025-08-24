package service

import (
	"context"
	"errors"
	"repair-service/domain"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// service implements the RepairService interface
type service struct {
	repo domain.RepairRepository
}

// NewService creates a new instance of the repair service
func NewService(repo domain.RepairRepository) *service {
	return &service{
		repo: repo,
	}
}

// CreateRepair creates a new repair request with the provided cost
func (s *service) CreateRepair(ctx context.Context, cost *domain.RepairCostModel) (*domain.RepairModel, error) {
	// Validate input
	if cost == nil || cost.UserID == "" || cost.RepairType == "" || cost.TotalPrice <= 0 {
		return nil, errors.New("invalid repair cost data")
	}

	// Save the repair cost
	err := s.repo.SaveRepairCost(ctx, cost)
	if err != nil {
		return nil, err
	}

	// Create a new repair with a unique ID
	repair := &domain.RepairModel{
		ID:         primitive.NewObjectID().Hex(), // Generate a new ID for the repair
		UserID:     cost.UserID,
		Status:     "pending",
		RepairCost: cost,
	}

	// Save the repair
	createdRepair, err := s.repo.CreateRepair(ctx, repair)
	if err != nil {
		return nil, err
	}

	return createdRepair, nil
}

// EstimateRepairCost generates an estimated cost for a repair based on repair type
func (s *service) EstimateRepairCost(ctx context.Context, repairType string, userID string) (*domain.RepairCostModel, error) {
	// Validate input
	if repairType == "" || userID == "" {
		return nil, errors.New("repair type and user ID are required")
	}

	// Simple cost estimation logic based on repair type
	totalPrice := 0.0
	switch repairType {
	case "flat_tire":
		totalPrice = 50.0
	case "brake_repair":
		totalPrice = 150.0
	case "chain_replacement":
		totalPrice = 80.0
	default:
		return nil, errors.New("unknown repair type")
	}

	// Create repair cost model
	cost := &domain.RepairCostModel{
		ID:         primitive.NewObjectID().Hex(), // Generate a new ID
		UserID:     userID,
		RepairType: repairType,
		TotalPrice: totalPrice,
	}

	return cost, nil
}

// GetAndValidateRepairCost retrieves a repair cost and validates it belongs to the user
func (s *service) GetAndValidateRepairCost(ctx context.Context, costID, userID string) (*domain.RepairCostModel, error) {
	// Validate input
	if costID == "" || userID == "" {
		return nil, errors.New("cost ID and user ID are required")
	}

	// Retrieve the repair cost
	cost, err := s.repo.GetRepairCostByID(ctx, costID)
	if err != nil {
		return nil, err
	}

	// Validate user ownership
	if cost.UserID != userID {
		return nil, errors.New("repair cost does not belong to the specified user")
	}

	return cost, nil
}

// GetRepairByID retrieves a repair by its ID
func (s *service) GetRepairByID(ctx context.Context, id string) (*domain.RepairModel, error) {
	// Validate input
	if id == "" {
		return nil, errors.New("repair ID is required")
	}

	// Retrieve the repair
	repair, err := s.repo.GetRepairByID(ctx, id)
	if err != nil {
		return nil, err
	}

	return repair, nil
}

// UpdateRepair updates the status of a repair
func (s *service) UpdateRepair(ctx context.Context, repairID string, status string) error {
	// Validate input
	if repairID == "" || status == "" {
		return errors.New("repair ID and status are required")
	}

	// Validate status
	validStatuses := map[string]bool{
		"pending":     true,
		"in_progress": true,
		"completed":   true,
		"cancelled":   true,
	}
	if !validStatuses[status] {
		return errors.New("invalid status")
	}

	// Update the repair
	err := s.repo.UpdateRepair(ctx, repairID, status)
	if err != nil {
		return err
	}

	return nil
}
