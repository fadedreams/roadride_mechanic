package service

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"mechanic-service/domain"
	"mechanic-service/kafka"
	"os"

	"github.com/hashicorp/consul/api"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Service implements the business logic for the mechanic service
type Service struct {
	repo          domain.MechanicRepository
	tracer        trace.Tracer
	logger        *slog.Logger
	KafkaConsumer *kafka.Consumer
}

// NewService creates a new instance of the mechanic service
func NewService(repo domain.MechanicRepository, logger *slog.Logger) *Service {
	_, span := otel.Tracer("mechanic-service").Start(context.Background(), "InitializeService")
	defer span.End()

	// Initialize Consul client
	consulAddr := os.Getenv("CONSUL_ADDRESS")
	if consulAddr == "" {
		consulAddr = "consul:8500"
	}
	consulConfig := api.DefaultConfig()
	consulConfig.Address = consulAddr
	consulClient, err := api.NewClient(consulConfig)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to create Consul client")
		logger.Error("Failed to create Consul client", "error", err, "app", "mechanic-service")
		panic(fmt.Sprintf("failed to create Consul client: %v", err))
	}

	// Query Consul for Kafka service
	serviceName := "kafka"
	services, _, err := consulClient.Agent().Service(serviceName+"-9094", nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to query Consul for Kafka service")
		logger.Error("Failed to query Consul for Kafka service", "error", err, "app", "mechanic-service")
		panic(fmt.Sprintf("failed to query Consul for Kafka service: %v", err))
	}
	if services == nil {
		err := fmt.Errorf("Kafka service not found in Consul")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		logger.Error("Kafka service not found in Consul", "serviceName", serviceName, "app", "mechanic-service")
		panic("Kafka service not found in Consul")
	}
	bootstrapServers := fmt.Sprintf("%s:%d", services.Address, services.Port)
	span.SetAttributes(
		attribute.String("kafkaServiceName", serviceName),
		attribute.String("bootstrapServers", bootstrapServers),
	)
	logger.Info("Resolved Kafka service from Consul", "bootstrapServers", bootstrapServers, "app", "mechanic-service")

	// Initialize Kafka consumer
	consumer, err := kafka.NewConsumer(bootstrapServers, "http://schema-registry:8081", "repair-events", "mechanic-service-group", logger)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to initialize Kafka consumer")
		logger.Error("Failed to initialize Kafka consumer", "error", err, "app", "mechanic-service")
		panic(fmt.Sprintf("failed to initialize Kafka consumer: %v", err))
	}

	svc := &Service{
		repo:          repo,
		tracer:        otel.Tracer("mechanic-service"),
		logger:        logger,
		KafkaConsumer: consumer,
	}

	// Start Kafka consumer in a separate goroutine
	go func() {
		err := consumer.Start(context.Background(), svc.processRepairEvent)
		if err != nil {
			logger.Error("Kafka consumer stopped with error", "error", err, "app", "mechanic-service")
		}
	}()

	return svc
}

// processRepairEvent processes incoming Kafka repair events
func (s *Service) processRepairEvent(ctx context.Context, event *kafka.RepairEvent) error {
	_, span := s.tracer.Start(ctx, "ProcessRepairEvent")
	defer span.End()

	s.logger.Info("Processing repair event",
		"repairID", event.ID,
		"status", event.Status,
		"repairType", event.RepairType,
		"totalPrice", event.TotalPrice,
		"app", "mechanic-service")
	span.SetAttributes(
		attribute.String("repairID", event.ID),
		attribute.String("status", event.Status),
		attribute.String("repairType", event.RepairType),
		attribute.Float64("totalPrice", event.TotalPrice),
	)

	// Convert RepairEvent to domain.Repair
	var userLocation *domain.Location
	if event.UserLocation != nil {
		userLocation = &domain.Location{
			Longitude: event.UserLocation.Longitude,
			Latitude:  event.UserLocation.Latitude,
		}
	}

	mechanics := make([]domain.MechanicInfo, len(event.Mechanics))
	for i, m := range event.Mechanics {
		mechanics[i] = domain.MechanicInfo{
			ID:       m.ID,
			Name:     m.Name,
			Location: domain.Location{
				Longitude: m.Location.Longitude,
				Latitude:  m.Location.Latitude,
			},
			Distance: m.Distance,
		}
	}

	repair := &domain.Repair{
		ID:     event.ID,
		UserID: event.UserID,
		Status: event.Status,
		RepairCost: &domain.RepairCost{
			ID:           event.ID, // Assuming same ID for simplicity
			UserID:       event.UserID,
			RepairType:   event.RepairType,
			TotalPrice:   event.TotalPrice,
			UserLocation: userLocation,
			Mechanics:    mechanics,
		},
	}

	// Check if repair already exists in the database
	existing, err := s.repo.GetAllRepairs(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to check existing repairs")
		s.logger.Error("Failed to check existing repairs", "error", err, "app", "mechanic-service")
		return err
	}

	for _, r := range existing {
		if r.ID == repair.ID {
			s.logger.Info("Repair already exists, skipping", "repairID", repair.ID, "app", "mechanic-service")
			return nil
		}
	}

	// Store the repair in the database
	_, err = s.repo.(*domain.MongoRepository).RepairCollection.InsertOne(ctx, repair)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to store repair")
		s.logger.Error("Failed to store repair", "repairID", repair.ID, "error", err, "app", "mechanic-service")
		return err
	}

	s.logger.Info("Stored repair from Kafka event", "repairID", repair.ID, "app", "mechanic-service")
	return nil
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
