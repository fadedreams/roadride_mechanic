package service

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"mechanic-service/domain"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Service implements the business logic for the mechanic service
type Service struct {
	repo   domain.MechanicRepository
	tracer trace.Tracer
	logger *slog.Logger
}

// NewService creates a new instance of the mechanic service
func NewService(repo domain.MechanicRepository, logger *slog.Logger) *Service {
	return &Service{
		repo:   repo,
		tracer: otel.Tracer("mechanic-service"),
		logger: logger,
	}
}

// haversine calculates the distance between two points in kilometers
func (s *Service) haversine(l1, l2 domain.Location) float64 {
	const R = 6371 // Earth's radius in km
	lat1 := l1.Latitude * math.Pi / 180
	lat2 := l2.Latitude * math.Pi / 180
	dLat := (l2.Latitude - l1.Latitude) * math.Pi / 180
	dLon := (l2.Longitude - l1.Longitude) * math.Pi / 180

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}

// ListNearbyRepairs lists repairs within 10km of a specified mechanic's location
func (s *Service) ListNearbyRepairs(ctx context.Context, mechanicID string) ([]*domain.Repair, error) {
	ctx, span := s.tracer.Start(ctx, "ServiceListNearbyRepairs")
	defer span.End()

	if mechanicID == "" {
		err := fmt.Errorf("mechanic ID is required")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error("Mechanic ID is required", "app", "mechanic-service")
		return nil, err
	}

	// Get mechanic details
	mechanic, err := s.repo.GetMechanicByID(ctx, mechanicID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find mechanic")
		s.logger.Error("Failed to find mechanic", "error", err, "mechanicID", mechanicID, "app", "mechanic-service")
		return nil, fmt.Errorf("failed to find mechanic: %w", err)
	}
	mechanicLoc := mechanic.Location
	span.SetAttributes(
		attribute.String("mechanicID", mechanicID),
		attribute.Float64("mechanic.latitude", mechanicLoc.Latitude),
		attribute.Float64("mechanic.longitude", mechanicLoc.Longitude),
	)

	// Get all repairs
	repairs, err := s.repo.GetAllRepairs(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to query repairs")
		s.logger.Error("Failed to query repairs", "error", err, "app", "mechanic-service")
		return nil, fmt.Errorf("failed to query repairs: %w", err)
	}

	var nearby []*domain.Repair
	for _, repair := range repairs {
		if repair.RepairCost != nil && repair.RepairCost.UserLocation != nil {
			distance := s.haversine(mechanicLoc, *repair.RepairCost.UserLocation)
			if distance <= 10 {
				nearby = append(nearby, repair)
			}
		}
	}
	span.SetAttributes(attribute.Int("nearbyRepairCount", len(nearby)))
	s.logger.Info("Listed nearby repairs", "repairCount", len(nearby), "mechanicID", mechanicID, "app", "mechanic-service")

	return nearby, nil
}

// AssignRepair assigns a mechanic to a repair
func (s *Service) AssignRepair(ctx context.Context, repairID, mechanicID string) (*domain.Repair, error) {
	ctx, span := s.tracer.Start(ctx, "ServiceAssignRepair")
	defer span.End()

	if repairID == "" || mechanicID == "" {
		err := fmt.Errorf("repair ID and mechanic ID are required")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error("Repair ID and mechanic ID are required", "repairID", repairID, "mechanicID", mechanicID, "app", "mechanic-service")
		return nil, err
	}

	// Validate mechanic
	_, err := s.repo.GetMechanicByID(ctx, mechanicID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find mechanic")
		s.logger.Error("Failed to find mechanic", "error", err, "mechanicID", mechanicID, "app", "mechanic-service")
		return nil, fmt.Errorf("failed to find mechanic: %w", err)
	}

	// Assign the repair
	repair, err := s.repo.AssignRepair(ctx, repairID, mechanicID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to assign repair")
		s.logger.Error("Failed to assign repair", "error", err, "repairID", repairID, "mechanicID", mechanicID, "app", "mechanic-service")
		return nil, fmt.Errorf("failed to assign repair: %w", err)
	}

	s.logger.Info("Assigned repair", "repairID", repairID, "mechanicID", mechanicID, "app", "mechanic-service")
	span.SetAttributes(
		attribute.String("repairID", repairID),
		attribute.String("mechanicID", mechanicID),
	)
	return repair, nil
}
