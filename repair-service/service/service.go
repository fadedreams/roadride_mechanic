package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"repair-service/domain"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// service implements the RepairService interface
type service struct {
	repo domain.RepairRepository
	httpClient *http.Client
}

// NewService creates a new instance of the repair service
func NewService(repo domain.RepairRepository) *service {
	return &service{
		repo:       repo,
		httpClient: &http.Client{Timeout: 10 * time.Second},
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
		ID:         primitive.NewObjectID().Hex(),
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

// EstimateRepairCost generates an estimated cost and mechanic distances
func (s *service) EstimateRepairCost(ctx context.Context, repairType string, userID string, userLocation *domain.Location) (*domain.RepairCostModel, error) {
	// Validate input
	if repairType == "" || userID == "" || userLocation == nil {
		return nil, errors.New("repair type, user ID, and location are required")
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

	// Get all mechanics
	mechanics, err := s.repo.GetAllMechanics(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get mechanics: %v", err)
	}

	// Prepare coordinates for OSRM table request
	coordinates := []string{
		fmt.Sprintf("%f,%f", userLocation.Longitude, userLocation.Latitude),
	}
	for _, mechanic := range mechanics {
		coordinates = append(coordinates, fmt.Sprintf("%f,%f", mechanic.Location.Longitude, mechanic.Location.Latitude))
	}

	// Call OSRM table service
	osrmURL := fmt.Sprintf("http://router.project-osrm.org/table/v1/driving/%s?sources=0", strings.Join(coordinates, ";"))
	resp, err := s.httpClient.Get(osrmURL)
	if err != nil {
		return nil, fmt.Errorf("failed to call OSRM table service: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OSRM table service returned status %d", resp.StatusCode)
	}

	var osrmResp struct {
		Code      string      `json:"code"`
		Durations [][]float64 `json:"durations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&osrmResp); err != nil {
		return nil, fmt.Errorf("failed to decode OSRM response: %v", err)
	}
	if osrmResp.Code != "Ok" {
		return nil, fmt.Errorf("OSRM table service returned code: %s", osrmResp.Code)
	}

	// Create mechanic info with distances (convert duration in seconds to distance in meters, assuming average speed of 50 km/h)
	var mechanicInfos []domain.MechanicInfo
	for i, mechanic := range mechanics {
		if i+1 >= len(osrmResp.Durations[0]) {
			continue // Skip if no duration data
		}
		duration := osrmResp.Durations[0][i+1] // Duration from user (source 0) to mechanic
		distance := duration * (50000.0 / 3600.0) // Convert seconds to meters (50 km/h = 50000 m/3600 s)
		mechanicInfos = append(mechanicInfos, domain.MechanicInfo{
			ID:       mechanic.ID,
			Name:     mechanic.Name,
			Location: mechanic.Location,
			Distance: distance,
		})
	}

	// Sort mechanics by distance
	sort.Slice(mechanicInfos, func(i, j int) bool {
		return mechanicInfos[i].Distance < mechanicInfos[j].Distance
	})

	// Create repair cost model
	cost := &domain.RepairCostModel{
		ID:           primitive.NewObjectID().Hex(),
		UserID:       userID,
		RepairType:   repairType,
		TotalPrice:   totalPrice,
		UserLocation: userLocation,
		Mechanics:    mechanicInfos,
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
