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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// service implements the RepairService interface
type service struct {
	repo domain.RepairRepository
	httpClient *http.Client
	tracer otel.Tracer
}

// NewService creates a new instance of the repair service
func NewService(repo domain.RepairRepository) *service {
	return &service{
		repo:       repo,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		tracer:     otel.Tracer("repair-service"),
	}
}

// CreateRepair creates a new repair request with the provided cost
func (s *service) CreateRepair(ctx context.Context, cost *domain.RepairCostModel) (*domain.RepairModel, error) {
	_, span := s.tracer.Start(ctx, "ServiceCreateRepair")
	defer span.End()

	// Validate input
	if cost == nil || cost.UserID == "" || cost.RepairType == "" || cost.TotalPrice <= 0 {
		err := errors.New("invalid repair cost data")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(
		attribute.String("userID", cost.UserID),
		attribute.String("repairType", cost.RepairType),
		attribute.Float64("totalPrice", cost.TotalPrice),
	)

	// Save the repair cost
	err := s.repo.SaveRepairCost(ctx, cost)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to save repair cost")
		return nil, err
	}

	// Create a new repair with a unique ID
	repair := &domain.RepairModel{
		ID:         primitive.NewObjectID().Hex(),
		UserID:     cost.UserID,
		Status:     "pending",
		RepairCost: cost,
	}
	span.SetAttributes(attribute.String("repairID", repair.ID))

	// Save the repair
	createdRepair, err := s.repo.CreateRepair(ctx, repair)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to create repair")
		return nil, err
	}

	return createdRepair, nil
}

// EstimateRepairCost generates an estimated cost and mechanic distances
func (s *service) EstimateRepairCost(ctx context.Context, repairType string, userID string, userLocation *domain.Location) (*domain.RepairCostModel, error) {
	ctx, span := s.tracer.Start(ctx, "ServiceEstimateRepairCost")
	defer span.End()

	// Validate input
	if repairType == "" || userID == "" || userLocation == nil {
		err := errors.New("repair type, user ID, and location are required")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(
		attribute.String("repairType", repairType),
		attribute.String("userID", userID),
		attribute.Float64("location.longitude", userLocation.Longitude),
		attribute.Float64("location.latitude", userLocation.Latitude),
	)

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
		err := errors.New("unknown repair type")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(attribute.Float64("totalPrice", totalPrice))

	// Get all mechanics
	mechanics, err := s.repo.GetAllMechanics(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to get mechanics")
		return nil, fmt.Errorf("failed to get mechanics: %v", err)
	}
	span.SetAttributes(attribute.Int("mechanicCount", len(mechanics)))

	// Prepare coordinates for OSRM table request
	coordinates := []string{
		fmt.Sprintf("%f,%f", userLocation.Longitude, userLocation.Latitude),
	}
	for _, mechanic := range mechanics {
		coordinates = append(coordinates, fmt.Sprintf("%f,%f", mechanic.Location.Longitude, mechanic.Location.Latitude))
	}

	// Call OSRM table service
	osrmURL := fmt.Sprintf("http://router.project-osrm.org/table/v1/driving/%s?sources=0", strings.Join(coordinates, ";"))
	req, err := http.NewRequestWithContext(ctx, "GET", osrmURL, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to create OSRM request")
		return nil, fmt.Errorf("failed to create OSRM request: %v", err)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	_, osrmSpan := s.tracer.Start(ctx, "OSRMTableRequest")
	resp, err := s.httpClient.Do(req)
	osrmSpan.End()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to call OSRM table service")
		return nil, fmt.Errorf("failed to call OSRM table service: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("OSRM table service returned status %d", resp.StatusCode)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	var osrmResp struct {
		Code      string      `json:"code"`
		Durations [][]float64 `json:"durations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&osrmResp); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to decode OSRM response")
		return nil, fmt.Errorf("failed to decode OSRM response: %v", err)
	}
	if osrmResp.Code != "Ok" {
		err := fmt.Errorf("OSRM table service returned code: %s", osrmResp.Code)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
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
	span.SetAttributes(attribute.String("costID", cost.ID))

	return cost, nil
}

// GetAndValidateRepairCost retrieves a repair cost and validates it belongs to the user
func (s *service) GetAndValidateRepairCost(ctx context.Context, costID, userID string) (*domain.RepairCostModel, error) {
	_, span := s.tracer.Start(ctx, "ServiceGetAndValidateRepairCost")
	defer span.End()

	// Validate input
	if costID == "" || userID == "" {
		err := errors.New("cost ID and user ID are required")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(
		attribute.String("costID", costID),
		attribute.String("userID", userID),
	)

	// Retrieve the repair cost
	cost, err := s.repo.GetRepairCostByID(ctx, costID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to get repair cost")
		return nil, err
	}

	// Validate user ownership
	if cost.UserID != userID {
		err := errors.New("repair cost does not belong to the specified user")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	return cost, nil
}

// GetRepairByID retrieves a repair by its ID
func (s *service) GetRepairByID(ctx context.Context, id string) (*domain.RepairModel, error) {
	_, span := s.tracer.Start(ctx, "ServiceGetRepairByID")
	defer span.End()

	// Validate input
	if id == "" {
		err := errors.New("repair ID is required")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(attribute.String("repairID", id))

	// Retrieve the repair
	repair, err := s.repo.GetRepairByID(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to get repair")
		return nil, err
	}

	return repair, nil
}

// UpdateRepair updates the status of a repair
func (s *service) UpdateRepair(ctx context.Context, repairID string, status string) error {
	_, span := s.tracer.Start(ctx, "ServiceUpdateRepair")
	defer span.End()

	// Validate input
	if repairID == "" || status == "" {
		err := errors.New("repair ID and status are required")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetAttributes(
		attribute.String("repairID", repairID),
		attribute.String("status", status),
	)

	// Validate status
	validStatuses := map[string]bool{
		"pending":     true,
		"in_progress": true,
		"completed":   true,
		"cancelled":   true,
	}
	if !validStatuses[status] {
		err := errors.New("invalid status")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Update the repair
	err := s.repo.UpdateRepair(ctx, repairID, status)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to update repair")
		return err
	}

	return nil
}
