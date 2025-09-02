package kafka

import (
	"context"
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
	SchemaID      int
	topic         string
	logger        *slog.Logger
	tracer        trace.Tracer
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
	logger.Info("Schema registered", "schemaID", schemaObj.ID(), "app", "repair-service")

	return &Producer{
		kafkaProducer: p,
		srClient:      srClient,
		schema:        schema,
		SchemaID:      schemaObj.ID(),
		topic:         topic,
		logger:        logger,
		tracer:        otel.Tracer("repair-service"),
	}, nil
}

// PublishOutboxEvent publishes an outbox event to Kafka
func (p *Producer) PublishOutboxEvent(ctx context.Context, event *domain.OutboxEvent) error {
	_, span := p.tracer.Start(ctx, "PublishOutboxEvent")
	defer span.End()

	// Publish to Kafka
	deliveryChan := make(chan kafka.Event)
	err := p.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          event.Payload,
	}, deliveryChan)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to produce message")
		p.logger.Error("Failed to produce message", "eventID", event.ID, "error", err, "app", "repair-service")
		return fmt.Errorf("failed to produce message: %w", err)
	}

	// Wait for delivery report
	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		span.RecordError(m.TopicPartition.Error)
		span.SetStatus(codes.Error, "Delivery failed")
		p.logger.Error("Delivery failed", "eventID", event.ID, "error", m.TopicPartition.Error, "app", "repair-service")
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}
	p.logger.Info("Published outbox event",
		"eventID", event.ID,
		"topic", *m.TopicPartition.Topic,
		"partition", m.TopicPartition.Partition,
		"offset", m.TopicPartition.Offset,
		"app", "repair-service")
	span.SetAttributes(
		attribute.String("eventID", event.ID),
		attribute.String("topic", *m.TopicPartition.Topic),
		attribute.Int("partition", int(m.TopicPartition.Partition)),
		attribute.Int64("offset", int64(m.TopicPartition.Offset)),
	)

	close(deliveryChan)
	return nil
}

// Close shuts down the Kafka producer
func (p *Producer) Close() {
	p.logger.Info("Closing Kafka producer", "app", "repair-service")
	p.kafkaProducer.Close()
}
