package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"repair-service/domain"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
)

type Producer struct {
	Producer       *kafka.Producer
	schemaRegistry *srclient.SchemaRegistryClient
	schema         avro.Schema
	schemaID       int
	logger         *slog.Logger
	tracer     trace.Tracer
}

func NewProducer(logger *slog.Logger) (*Producer, error) {
	// Initialize Schema Registry client
	client := srclient.CreateSchemaRegistryClient("http://schema-registry:8081")

	// Load Avro schema
	schemaBytes, err := os.ReadFile("repair.avsc")
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %v", err)
	}
	schemaStr := string(schemaBytes)
	schema, err := avro.Parse(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema: %v", err)
	}

	// Register schema
	schemaObj, err := client.CreateSchema("repair-events-value", schemaStr, srclient.Avro)
	if err != nil {
		return nil, fmt.Errorf("failed to register schema: %v", err)
	}

	// Initialize Kafka producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "broker:9092",
		"compression.type":  "snappy",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %v", err)
	}

	return &Producer{
		producer:       p,
		schemaRegistry: client,
		schema:         schema,
		schemaID:       schemaObj.ID(),
		logger:         logger,
		tracer:         otel.Tracer("repair-service"),
	}, nil
}

func (p *Producer) Close() {
	p.producer.Close()
}

func (p *Producer) PublishRepair(ctx context.Context, repair *domain.RepairModel) error {
	_, span := p.tracer.Start(ctx, "KafkaPublishRepair")
	defer span.End()

	topic := "repair-events"

	// Serialize to Avro
	payload, err := avro.Marshal(p.schema, repair)
	if err != nil {
		span.RecordError(err)
		p.logger.Error("Failed to serialize repair", "error", err)
		return fmt.Errorf("failed to serialize repair: %v", err)
	}

	// Add Schema Registry wire format: magic byte (0) + 4-byte schema ID
	encodedPayload := make([]byte, 5+len(payload))
	encodedPayload[0] = 0
	binary.BigEndian.PutUint32(encodedPayload[1:5], uint32(p.schemaID))
	copy(encodedPayload[5:], payload)

	// Delivery report channel
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	// Produce message
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          encodedPayload,
	}, deliveryChan)
	if err != nil {
		span.RecordError(err)
		p.logger.Error("Failed to produce message", "error", err)
		return fmt.Errorf("failed to produce message: %v", err)
	}

	// Wait for delivery report
	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		span.RecordError(m.TopicPartition.Error)
		p.logger.Error("Delivery failed", "error", m.TopicPartition.Error)
		return fmt.Errorf("delivery failed: %v", m.TopicPartition.Error)
	}

	p.logger.Info("Delivered message",
		"topic", *m.TopicPartition.Topic,
		"partition", m.TopicPartition.Partition,
		"offset", m.TopicPartition.Offset,
		"repairID", repair.ID)
	span.SetAttributes(
		attribute.String("repairID", repair.ID),
		attribute.String("topic", *m.TopicPartition.Topic),
	)

	return nil
}
