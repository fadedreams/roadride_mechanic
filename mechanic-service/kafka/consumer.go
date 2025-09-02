package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// RepairEvent mirrors the Avro schema from repair-service
type RepairEvent struct {
	ID          string         `avro:"id"`
	UserID      string         `avro:"user_id"`
	Status      string         `avro:"status"`
	RepairType  string         `avro:"repair_type"`
	TotalPrice  float64        `avro:"total_price"`
	UserLocation *Location      `avro:"user_location"`
	Mechanics   []MechanicInfo `avro:"mechanics"`
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

type Consumer struct {
	kafkaConsumer *kafka.Consumer
	srClient      *srclient.SchemaRegistryClient
	schema        avro.Schema
	topic         string
	logger        *slog.Logger
	tracer        trace.Tracer
}

func NewConsumer(bootstrapServers, schemaRegistryURL, topic, groupID string, logger *slog.Logger) (*Consumer, error) {
	// Initialize Kafka consumer
	config := &kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
	}
	c, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Initialize Schema Registry client
	srClient := srclient.CreateSchemaRegistryClient(schemaRegistryURL)

	// Load Avro schema
	schemaBytes, err := os.ReadFile("repair_event.avsc")
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}
	schema, err := avro.Parse(string(schemaBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema: %w", err)
	}

	return &Consumer{
		kafkaConsumer: c,
		srClient:      srClient,
		schema:        schema,
		topic:         topic,
		logger:        logger,
		tracer:        otel.Tracer("mechanic-service"),
	}, nil
}

// Start begins consuming messages from the Kafka topic
func (c *Consumer) Start(ctx context.Context, processFunc func(context.Context, *RepairEvent) error) error {
	_, span := c.tracer.Start(ctx, "KafkaConsumerStart")
	defer span.End()

	// Subscribe to the topic
	err := c.kafkaConsumer.SubscribeTopics([]string{c.topic}, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to subscribe to topic")
		c.logger.Error("Failed to subscribe to topic", "topic", c.topic, "error", err, "app", "mechanic-service")
		return fmt.Errorf("failed to subscribe to topic: %w", err)
	}
	c.logger.Info("Subscribed to Kafka topic", "topic", c.topic, "app", "mechanic-service")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Context canceled, stopping Kafka consumer", "app", "mechanic-service")
			return ctx.Err()
		default:
			msg, err := c.kafkaConsumer.ReadMessage(-1)
			if err != nil {
				c.logger.Error("Error reading Kafka message", "error", err, "app", "mechanic-service")
				continue
			}

			_, span := c.tracer.Start(ctx, "ProcessKafkaMessage")
			// Deserialize Avro message
			if len(msg.Value) < 5 {
				span.RecordError(fmt.Errorf("invalid message length"))
				span.SetStatus(codes.Error, "Invalid message length")
				c.logger.Error("Invalid message length", "length", len(msg.Value), "app", "mechanic-service")
				span.End()
				continue
			}

			// Extract schema ID (skip magic byte)
			schemaID := int(binary.BigEndian.Uint32(msg.Value[1:5]))
			span.SetAttributes(
				attribute.String("topic", *msg.TopicPartition.Topic),
				attribute.Int("partition", int(msg.TopicPartition.Partition)),
				attribute.Int64("offset", int64(msg.TopicPartition.Offset)),
				attribute.Int("schemaID", schemaID),
			)

			// Fetch schema if not already loaded
			if c.schema == nil {
				schemaObj, err := c.srClient.GetSchema(schemaID)
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, "Failed to fetch schema")
					c.logger.Error("Failed to fetch schema", "schemaID", schemaID, "error", err, "app", "mechanic-service")
					span.End()
					continue
				}
				c.schema, err = avro.Parse(schemaObj.Schema())
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, "Failed to parse schema")
					c.logger.Error("Failed to parse schema", "schemaID", schemaID, "error", err, "app", "mechanic-service")
					span.End()
					continue
				}
			}

			// Deserialize message
			var event RepairEvent
			err = avro.Unmarshal(c.schema, msg.Value[5:], &event)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "Failed to deserialize message")
				c.logger.Error("Failed to deserialize message", "error", err, "app", "mechanic-service")
				span.End()
				continue
			}

			// Process the event
			err = processFunc(ctx, &event)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "Failed to process event")
				c.logger.Error("Failed to process event", "repairID", event.ID, "error", err, "app", "mechanic-service")
				span.End()
				continue
			}

			c.logger.Info("Processed repair event",
				"repairID", event.ID,
				"status", event.Status,
				"topic", *msg.TopicPartition.Topic,
				"partition", msg.TopicPartition.Partition,
				"offset", msg.TopicPartition.Offset,
				"app", "mechanic-service")
			span.End()
		}
	}
}

// Close shuts down the Kafka consumer
func (c *Consumer) Close() {
	c.logger.Info("Closing Kafka consumer", "app", "mechanic-service")
	c.kafkaConsumer.Close()
}
