package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"mechanic-service/domain"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"log/slog"
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
	repo          domain.MechanicRepository
}

func NewConsumer(bootstrapServers, schemaRegistryURL, topic, groupID string, logger *slog.Logger, repo domain.MechanicRepository) (*Consumer, error) {
	// Initialize Kafka consumer
	config := &kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false, // Disable auto-commit to control commits
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
		repo:          repo,
	}, nil
}

// Start begins consuming messages from the Kafka topic
func (c *Consumer) Start(ctx context.Context) error {
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

			// Store event in outbox
			session, err := c.repo.GetMongoClient(ctx).StartSession()
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "Failed to start MongoDB session")
				c.logger.Error("Failed to start MongoDB session", "error", err, "app", "mechanic-service")
				span.End()
				continue
			}
			defer session.EndSession(ctx)

			err = session.StartTransaction()
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "Failed to start transaction")
				c.logger.Error("Failed to start transaction", "error", err, "app", "mechanic-service")
				span.End()
				continue
			}

			err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
				outboxEvent := &domain.OutboxEvent{
					ID:        primitive.NewObjectID().Hex(),
					EventType: "RepairEvent",
					Payload:   msg.Value,
					CreatedAt: time.Now(),
					Processed: false,
				}
				if err := c.repo.SaveOutboxEvent(sc, outboxEvent); err != nil {
					return fmt.Errorf("failed to save outbox event: %w", err)
				}
				c.logger.Info("Saved outbox event in transaction", "eventID", outboxEvent.ID, "app", "mechanic-service")
				return nil
			})
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "Transaction failed")
				c.logger.Error("Transaction failed", "error", err, "app", "mechanic-service")
				session.AbortTransaction(ctx)
				span.End()
				continue
			}

			if err := session.CommitTransaction(ctx); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "Failed to commit transaction")
				c.logger.Error("Failed to commit transaction", "error", err, "app", "mechanic-service")
				span.End()
				continue
			}

			// Commit Kafka offset
			_, err = c.kafkaConsumer.CommitMessage(msg)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "Failed to commit Kafka offset")
				c.logger.Error("Failed to commit Kafka offset", "topic", *msg.TopicPartition.Topic, "partition", msg.TopicPartition.Partition, "offset", msg.TopicPartition.Offset, "error", err, "app", "mechanic-service")
				span.End()
				continue
			}

			c.logger.Info("Committed Kafka message and outbox event",
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
