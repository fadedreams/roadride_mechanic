package service

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"repair-service/domain"
	"repair-service/kafka"
	"sort"
	"strings"
	"time"

	"log/slog"

	"github.com/hamba/avro/v2"
	"github.com/hashicorp/consul/api"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// service implements the RepairService interface
type service struct {
	repo           domain.RepairRepository
	httpClient     *http.Client
	tracer         trace.Tracer
	logger         *slog.Logger
	KafkaProducer  *kafka.Producer
	outboxProcessor *kafka.OutboxProcessor
}

// NewService creates a new instance of the repair service
func NewService(repo domain.RepairRepository, logger *slog.Logger) *service {
	_, span := otel.Tracer("repair-service").Start(context.Background(), "InitializeService")
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
		logger.Error("Failed to create Consul client", "error", err, "app", "repair-service")
		panic(fmt.Sprintf("failed to create Consul client: %v", err))
	}

	// Query Consul for Kafka service
	serviceName := "kafka"
	services, _, err := consulClient.Agent().Service(serviceName+"-9094", nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to query Consul for Kafka service")
		logger.Error("Failed to query Consul for Kafka service", "error", err, "app", "repair-service")
		panic(fmt.Sprintf("failed to query Consul for Kafka service: %v", err))
	}
	if services == nil {
		err := errors.New("Kafka service not found in Consul")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		logger.Error("Kafka service not found in Consul", "serviceName", serviceName, "app", "repair-service")
		panic("Kafka service not found in Consul")
	}
	bootstrapServers := fmt.Sprintf("%s:%d", services.Address, services.Port)
	span.SetAttributes(
		attribute.String("kafkaServiceName", serviceName),
		attribute.String("bootstrapServers", bootstrapServers),
	)
	logger.Info("Resolved Kafka service from Consul", "bootstrapServers", bootstrapServers, "app", "repair-service")

	// Initialize Kafka producer with resolved bootstrap servers
	kafkaProducer, err := kafka.NewProducer(bootstrapServers, "http://schema-registry:8081", "repair-events", logger)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to initialize Kafka producer")
		logger.Error("Failed to initialize Kafka producer", "error", err, "app", "repair-service")
		panic(fmt.Sprintf("failed to initialize Kafka producer: %v", err))
	}

	svc := &service{
		repo:          repo,
		httpClient:    &http.Client{Timeout: 10 * time.Second},
		tracer:        otel.Tracer("repair-service"),
		logger:        logger,
		KafkaProducer: kafkaProducer,
		outboxProcessor: kafka.NewOutboxProcessor(repo, kafkaProducer, logger),
	}

	// Start outbox processor in a separate goroutine
	go func() {
		err := svc.outboxProcessor.Start(context.Background())
		if err != nil {
			logger.Error("Outbox processor stopped with error", "error", err, "app", "repair-service")
		}
	}()

	return svc
}

// CreateRepair creates a new repair request with the provided cost
func (s *service) CreateRepair(ctx context.Context, cost *domain.RepairCostModel) (*domain.RepairModel, error) {
	_, span := s.tracer.Start(ctx, "ServiceCreateRepair")
	defer span.End()

	if cost == nil || cost.UserID == "" || cost.RepairType == "" || cost.TotalPrice <= 0 {
		err := errors.New("invalid repair cost data")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error("Invalid repair cost data", "error", err, "app", "repair-service")
		return nil, err
	}
	span.SetAttributes(
		attribute.String("userID", cost.UserID),
		attribute.String("repairType", cost.RepairType),
		attribute.Float64("totalPrice", cost.TotalPrice),
	)

	repair := &domain.RepairModel{
		ID:         primitive.NewObjectID().Hex(),
		UserID:     cost.UserID,
		Status:     "pending",
		RepairCost: cost,
	}
	span.SetAttributes(attribute.String("repairID", repair.ID))

	// Convert domain.RepairModel to kafka.RepairEvent
	event := &kafka.RepairEvent{
		ID:         repair.ID,
		UserID:     repair.UserID,
		Status:     repair.Status,
		RepairType: repair.RepairCost.RepairType,
		TotalPrice: repair.RepairCost.TotalPrice,
	}
	if repair.RepairCost.UserLocation != nil {
		event.UserLocation = &kafka.Location{
			Longitude: repair.RepairCost.UserLocation.Longitude,
			Latitude:  repair.RepairCost.UserLocation.Latitude,
		}
	}
	for _, m := range repair.RepairCost.Mechanics {
		event.Mechanics = append(event.Mechanics, kafka.MechanicInfo{
			ID:   m.ID,
			Name: m.Name,
			Location: kafka.Location{
				Longitude: m.Location.Longitude,
				Latitude:  m.Location.Latitude,
			},
			Distance: m.Distance,
		})
	}

	// Serialize to Avro
	schemaBytes, err := os.ReadFile("repair_event.avsc")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to read schema file")
		s.logger.Error("Failed to read schema file", "error", err, "app", "repair-service")
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}
	schema, err := avro.Parse(string(schemaBytes))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to parse schema")
		s.logger.Error("Failed to parse schema", "error", err, "app", "repair-service")
		return nil, fmt.Errorf("failed to parse schema: %w", err)
	}
	payload, err := avro.Marshal(schema, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to serialize event")
		s.logger.Error("Failed to serialize event", "error", err, "app", "repair-service")
		return nil, fmt.Errorf("failed to serialize event: %w", err)
	}

	// Add Schema Registry wire format: magic byte (0) + 4-byte schema ID
	encodedPayload := make([]byte, 5+len(payload))
	encodedPayload[0] = 0 // Magic byte
	binary.BigEndian.PutUint32(encodedPayload[1:5], uint32(s.KafkaProducer.SchemaID))
	copy(encodedPayload[5:], payload)

	// Save repair cost, repair, and outbox event in a transaction
	session, err := s.repo.(*domain.MongoRepository).RepairCollection.Database().Client().StartSession()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to start MongoDB session")
		s.logger.Error("Failed to start MongoDB session", "error", err, "app", "repair-service")
		return nil, fmt.Errorf("failed to start MongoDB session: %w", err)
	}
	defer session.EndSession(ctx)

	err = session.StartTransaction()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to start transaction")
		s.logger.Error("Failed to start transaction", "error", err, "app", "repair-service")
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		if err := s.repo.SaveRepairCost(sc, cost); err != nil {
			return fmt.Errorf("failed to save repair cost: %w", err)
		}
		s.logger.Info("Saved repair cost in transaction", "costID", cost.ID, "app", "repair-service")

		if _, err := s.repo.CreateRepair(sc, repair); err != nil {
			return fmt.Errorf("failed to create repair: %w", err)
		}
		s.logger.Info("Created repair in transaction", "repairID", repair.ID, "app", "repair-service")

		outboxEvent := &domain.OutboxEvent{
			ID:        primitive.NewObjectID().Hex(),
			EventType: "RepairCreated",
			Payload:   encodedPayload,
			CreatedAt: time.Now(),
			Processed: false,
		}
		if err := s.repo.SaveOutboxEvent(ctx, sc, outboxEvent); err != nil {
			return fmt.Errorf("failed to save outbox event: %w", err)
		}
		s.logger.Info("Saved outbox event in transaction", "eventID", outboxEvent.ID, "app", "repair-service")

		return nil
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Transaction failed")
		s.logger.Error("Transaction failed", "error", err, "app", "repair-service")
		session.AbortTransaction(ctx)
		return nil, err
	}

	if err := session.CommitTransaction(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to commit transaction")
		s.logger.Error("Failed to commit transaction", "error", err, "app", "repair-service")
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	s.logger.Info("Committed transaction for repair creation", "repairID", repair.ID, "app", "repair-service")
	return repair, nil
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
		s.logger.Error("Invalid input for estimate", "error", err, "app", "repair-service")
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
		s.logger.Error("Unknown repair type", "repairType", repairType, "app", "repair-service")
		return nil, err
	}
	span.SetAttributes(attribute.Float64("totalPrice", totalPrice))
	s.logger.Info("Estimated total price", "repairType", repairType, "totalPrice", totalPrice, "app", "repair-service")

	// Get all mechanics
	mechanics, err := s.repo.GetAllMechanics(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to get mechanics")
		s.logger.Error("Failed to get mechanics", "error", err, "app", "repair-service")
		return nil, fmt.Errorf("failed to get mechanics: %v", err)
	}
	span.SetAttributes(attribute.Int("mechanicCount", len(mechanics)))
	s.logger.Info("Retrieved mechanics", "count", len(mechanics), "app", "repair-service")

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
		s.logger.Error("Failed to create OSRM request", "error", err, "app", "repair-service")
		return nil, fmt.Errorf("failed to create OSRM request: %v", err)
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	_, osrmSpan := s.tracer.Start(ctx, "OSRMTableRequest")
	resp, err := s.httpClient.Do(req)
	osrmSpan.End()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to call OSRM table service")
		s.logger.Error("Failed to call OSRM table service", "error", err, "url", osrmURL, "app", "repair-service")
		return nil, fmt.Errorf("failed to call OSRM table service: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("OSRM table service returned status %d", resp.StatusCode)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error("OSRM table service error", "status_code", resp.StatusCode, "url", osrmURL, "app", "repair-service")
		return nil, err
	}

	var osrmResp struct {
		Code      string      `json:"code"`
		Durations [][]float64 `json:"durations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&osrmResp); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to decode OSRM response")
		s.logger.Error("Failed to decode OSRM response", "error", err, "app", "repair-service")
		return nil, fmt.Errorf("failed to decode OSRM response: %v", err)
	}
	if osrmResp.Code != "Ok" {
		err := fmt.Errorf("OSRM table service returned code: %s", osrmResp.Code)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error("OSRM table service returned non-OK code", "code", osrmResp.Code, "app", "repair-service")
		return nil, err
	}

	// Create mechanic info with distances (convert duration in seconds to distance in meters, assuming average speed of 50 km/h)
	var mechanicInfos []domain.MechanicInfo
	for i, mechanic := range mechanics {
		if i+1 >= len(osrmResp.Durations[0]) {
			s.logger.Warn("Skipping mechanic due to missing duration data", "mechanicID", mechanic.ID, "app", "repair-service")
			continue
		}
		duration := osrmResp.Durations[0][i+1]
		distance := duration * (50000.0 / 3600.0)
		mechanicInfos = append(mechanicInfos, domain.MechanicInfo{
			ID:       mechanic.ID,
			Name:     mechanic.Name,
			Location: mechanic.Location,
			Distance: distance,
		})
	}
	s.logger.Info("Calculated distances for mechanics", "count", len(mechanicInfos), "app", "repair-service")

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
	s.logger.Info("Created repair cost model", "costID", cost.ID, "app", "repair-service")

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
		s.logger.Error("Invalid input for get repair cost", "error", err, "app", "repair-service")
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
		s.logger.Error("Failed to get repair cost", "error", err, "app", "repair-service")
		return nil, err
	}
	s.logger.Info("Retrieved repair cost", "costID", costID, "app", "repair-service")

	// Validate user ownership
	if cost.UserID != userID {
		err := errors.New("repair cost does not belong to the specified user")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.logger.Error("Repair cost ownership validation failed", "costID", costID, "userID", userID, "app", "repair-service")
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
		s.logger.Error("Invalid repair ID", "error", err, "app", "repair-service")
		return nil, err
	}
	span.SetAttributes(attribute.String("repairID", id))

	// Retrieve the repair
	repair, err := s.repo.GetRepairByID(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to get repair")
		s.logger.Error("Failed to get repair", "error", err, "app", "repair-service")
		return nil, err
	}
	s.logger.Info("Retrieved repair", "repairID", id, "app", "repair-service")

	return repair, nil
}

// GetAllRepairs retrieves all repairs
func (s *service) GetAllRepairs(ctx context.Context) ([]*domain.RepairModel, error) {
	_, span := s.tracer.Start(ctx, "ServiceGetAllRepairs")
	defer span.End()

	// Retrieve all repairs
	repairs, err := s.repo.GetAllRepairs(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find repairs")
		s.logger.Error("Failed to find repairs", "error", err, "app", "repair-service")
		return nil, fmt.Errorf("failed to find repairs: %v", err)
	}
	s.logger.Info("Retrieved all repairs", "count", len(repairs), "app", "repair-service")

	span.SetAttributes(
		attribute.Int("repairCount", len(repairs)),
	)

	return repairs, nil
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
		s.logger.Error("Invalid input for update repair", "error", err, "app", "repair-service")
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
		s.logger.Error("Invalid status", "status", status, "app", "repair-service")
		return err
	}

	// Retrieve the repair to prepare the event
	repair, err := s.repo.GetRepairByID(ctx, repairID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to get repair for event")
		s.logger.Error("Failed to get repair for event", "error", err, "app", "repair-service")
		return err
	}

	// Update repair status and save outbox event in a transaction
	session, err := s.repo.(*domain.MongoRepository).RepairCollection.Database().Client().StartSession()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to start MongoDB session")
		s.logger.Error("Failed to start MongoDB session", "error", err, "app", "repair-service")
		return fmt.Errorf("failed to start MongoDB session: %w", err)
	}
	defer session.EndSession(ctx)

	err = session.StartTransaction()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to start transaction")
		s.logger.Error("Failed to start transaction", "error", err, "app", "repair-service")
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		if err := s.repo.UpdateRepair(sc, repairID, status); err != nil {
			return fmt.Errorf("failed to update repair: %w", err)
		}
		s.logger.Info("Updated repair in transaction", "repairID", repairID, "status", status, "app", "repair-service")

		// Update repair object for event
		repair.Status = status

		// Convert domain.RepairModel to kafka.RepairEvent
		event := &kafka.RepairEvent{
			ID:         repair.ID,
			UserID:     repair.UserID,
			Status:     repair.Status,
			RepairType: repair.RepairCost.RepairType,
			TotalPrice: repair.RepairCost.TotalPrice,
		}
		if repair.RepairCost.UserLocation != nil {
			event.UserLocation = &kafka.Location{
				Longitude: repair.RepairCost.UserLocation.Longitude,
				Latitude:  repair.RepairCost.UserLocation.Latitude,
			}
		}
		for _, m := range repair.RepairCost.Mechanics {
			event.Mechanics = append(event.Mechanics, kafka.MechanicInfo{
				ID:   m.ID,
				Name: m.Name,
				Location: kafka.Location{
					Longitude: m.Location.Longitude,
					Latitude:  m.Location.Latitude,
				},
				Distance: m.Distance,
			})
		}

		// Serialize to Avro
		schemaBytes, err := os.ReadFile("repair_event.avsc")
		if err != nil {
			return fmt.Errorf("failed to read schema file: %w", err)
		}
		schema, err := avro.Parse(string(schemaBytes))
		if err != nil {
			return fmt.Errorf("failed to parse schema: %w", err)
		}
		payload, err := avro.Marshal(schema, event)
		if err != nil {
			return fmt.Errorf("failed to serialize event: %w", err)
		}

		// Add Schema Registry wire format: magic byte (0) + 4-byte schema ID
		encodedPayload := make([]byte, 5+len(payload))
		encodedPayload[0] = 0 // Magic byte
		binary.BigEndian.PutUint32(encodedPayload[1:5], uint32(s.KafkaProducer.SchemaID))
		copy(encodedPayload[5:], payload)

		outboxEvent := &domain.OutboxEvent{
			ID:        primitive.NewObjectID().Hex(),
			EventType: "RepairUpdated",
			Payload:   encodedPayload,
			CreatedAt: time.Now(),
			Processed: false,
		}
		if err := s.repo.SaveOutboxEvent(ctx, sc, outboxEvent); err != nil {
			return fmt.Errorf("failed to save outbox event: %w", err)
		}
		s.logger.Info("Saved outbox event in transaction", "eventID", outboxEvent.ID, "app", "repair-service")

		return nil
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Transaction failed")
		s.logger.Error("Transaction failed", "error", err, "app", "repair-service")
		session.AbortTransaction(ctx)
		return err
	}

	if err := session.CommitTransaction(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to commit transaction")
		s.logger.Error("Failed to commit transaction", "error", err, "app", "repair-service")
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	s.logger.Info("Committed transaction for repair update", "repairID", repairID, "status", status, "app", "repair-service")
	return nil
}
