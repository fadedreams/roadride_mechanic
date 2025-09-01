package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"repair-service/domain"

	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// RepairEvent mirrors the Avro schema
type RepairEvent struct {
	ID          string               `avro:"id"`
	UserID      string               `avro:"user_id"`
	Status      string               `avro:"status"`
	RepairType  string               `avro:"repair_type"`
	TotalPrice  float64              `avro:"total_price"`
	UserLocation *Location           `avro:"user_location"`
	Mechanics   []MechanicInfo       `avro:"mechanics"`
}

type Location struct {
	Longitude float64 `avro:"longitude"`
	Latitude  float64 `avro:"latitude"`
}

type MechanicInfo struct {
	ID       string   `avro:"id"`
	Name     string   `avro:"name"`
	Location Location `avro:"location"`
	Distance float64  `avro:"distance"`
}

type Producer struct {
	kafkaProducer *kafka.Producer
	srClient      *srclient.SchemaRegistryClient
	schema        avro.Schema
	schemaID      int
	topic         string
	logger        *slog.Logger
	tracer     trace.Tracer
}

func NewProducer(bootstrapServers, schemaRegistryURL, topic string, logger *slog.Logger) (*Producer, error) {
	// Initialize Kafka producer
	config := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"compression.type":  "snappy",
	}
	p, err := kafka.NewProducer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Initialize Schema Registry client
	srClient := srclient.CreateSchemaRegistryClient(schemaRegistryURL)

	// Load Avro schema
	schemaBytes, err := os.ReadFile("repair_event.avsc")
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}
	schemaStr := string(schemaBytes)
	schema, err := avro.Parse(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema: %w", err)
	}

	// Register schema
	schemaObj, err := srClient.CreateSchema(topic+"-value", schemaStr, srclient.Avro)
	if err != nil {
		return nil, fmt.Errorf("failed to register schema: %w", err)
	}
	logger.Info("Schema registered", "schemaID", schemaObj.ID())

	return &Producer{
		kafkaProducer: p,
		srClient:      srClient,
		schema:        schema,
		schemaID:      schemaObj.ID(),
		topic:         topic,
		logger:        logger,
		tracer:        otel.Tracer("repair-service"),
	}, nil
}

func (p *Producer) PublishRepairEvent(ctx context.Context, repair *domain.RepairModel) error {
	_, span := p.tracer.Start(ctx, "PublishRepairEvent")
	defer span.End()

	// Convert domain.RepairModel to RepairEvent
	event := &RepairEvent{
		ID:         repair.ID,
		UserID:     repair.UserID,
		Status:     repair.Status,
		RepairType: repair.RepairCost.RepairType,
		TotalPrice: repair.RepairCost.TotalPrice,
	}
	if repair.RepairCost.UserLocation != nil {
		event.UserLocation = &Location{
			Longitude: repair.RepairCost.UserLocation.Longitude,
			Latitude:  repair.RepairCost.UserLocation.Latitude,
		}
	}
	for _, m := range repair.RepairCost.Mechanics {
		event.Mechanics = append(event.Mechanics, MechanicInfo{
			ID:   m.ID,
			Name: m.Name,
			Location: Location{
				Longitude: m.Location.Longitude,
				Latitude:  m.Location.Latitude,
			},
			Distance: m.Distance,
		})
	}
	span.SetAttributes(
		attribute.String("repairID", repair.ID),
		attribute.String("status", repair.Status),
	)

	// Serialize to Avro
	payload, err := avro.Marshal(p.schema, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to serialize event")
		p.logger.Error("Failed to serialize event", "error", err)
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	// Add Schema Registry wire format: magic byte (0) + 4-byte schema ID
	encodedPayload := make([]byte, 5+len(payload))
	encodedPayload[0] = 0 // Magic byte
	binary.BigEndian.PutUint32(encodedPayload[1:5], uint32(p.schemaID))
	copy(encodedPayload[5:], payload)

	// Publish to Kafka
	deliveryChan := make(chan kafka.Event)
	err = p.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          encodedPayload,
	}, deliveryChan)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to produce message")
		p.logger.Error("Failed to produce message", "error", err)
		return fmt.Errorf("failed to produce message: %w", err)
	}

	// Wait for delivery report
	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		span.RecordError(m.TopicPartition.Error)
		span.SetStatus(codes.Error, "Delivery failed")
		p.logger.Error("Delivery failed", "error", m.TopicPartition.Error)
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}
	p.logger.Info("Published repair event",
		"repairID", repair.ID,
		"topic", *m.TopicPartition.Topic,
		"partition", m.TopicPartition.Partition,
		"offset", m.TopicPartition.Offset)
	span.SetAttributes(
		attribute.String("topic", *m.TopicPartition.Topic),
		attribute.Int("partition", int(m.TopicPartition.Partition)),
		attribute.Int64("offset", int64(m.TopicPartition.Offset)),
	)

	close(deliveryChan)
	return nil
}

func (p *Producer) Close() {
	p.kafkaProducer.Close()
}
