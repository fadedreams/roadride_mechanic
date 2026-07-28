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

// slowProcessingThreshold is the point at which a single message's
// processing time gets logged as a warning. This is a canary for
// max.poll.interval.ms exhaustion, not a hard limit itself — tune it
// well below your configured max.poll.interval.ms so you get warned
// before a rebalance actually happens.
const slowProcessingThreshold = 10 * time.Second

func NewConsumer(bootstrapServers, schemaRegistryURL, topic, groupID string, logger *slog.Logger, repo domain.MechanicRepository) (*Consumer, error) {
	// Static group membership requires a stable per-instance identity.
	// This MUST be stable across restarts of the "same" logical instance,
	// and unique per running instance. In docker-compose this is a fixed
	// value tied to container_name; in the current single-replica k8s
	// Deployment it's also hardcoded. If you scale the Deployment beyond
	// one replica, switch to a StatefulSet and derive this from the
	// stable ordinal pod name instead (e.g. via the Kubernetes downward
	// API / POD_NAME), since a hardcoded value would collide across
	// replicas and a Deployment's generated pod names aren't stable
	// across restarts.
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		return nil, fmt.Errorf("INSTANCE_ID env var must be set (stable per-instance identity required for group.instance.id)")
	}

	// Initialize Kafka consumer
	config := &kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           groupID,
		"group.instance.id":  instanceID, // static membership: restarts are treated as reconnects, not leave+join, avoiding rebalance storms on rolling deploys/autoscaling
		"session.timeout.ms": 45000,      // grace window the broker waits before evicting a disconnected static member; must comfortably cover normal restart time
		"max.poll.interval.ms": 300000,   // ceiling on time between ReadMessage calls before the client voluntarily leaves the group; tune against measured p99 processing time (see processing-time logging in handleMessage)
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

// rebalanceCallback ensures that when this consumer is about to lose a
// partition, whatever has already been processed gets a final, blocking
// (synchronous) commit before the partition changes hands. This closes the
// window where a redelivery would happen simply because the last per-message
// commit hadn't been flushed yet.
func (c *Consumer) rebalanceCallback(kc *kafka.Consumer, event kafka.Event) error {
	switch e := event.(type) {
	case kafka.AssignedPartitions:
		c.logger.Info("Partitions assigned", "partitions", e.Partitions, "app", "mechanic-service")
		return kc.Assign(e.Partitions)

	case kafka.RevokedPartitions:
		c.logger.Info("Partitions revoked, forcing sync commit before giving them up",
			"partitions", e.Partitions, "app", "mechanic-service")
		if _, err := kc.Commit(); err != nil {
			// Not fatal: worst case is a harmless redelivery, which the
			// outbox idempotency check below already protects against.
			c.logger.Error("Failed to commit offsets on revoke", "error", err, "app", "mechanic-service")
		}
		return kc.Unassign()
	}
	return nil
}

// Start begins consuming messages from the Kafka topic
func (c *Consumer) Start(ctx context.Context) error {
	_, span := c.tracer.Start(ctx, "KafkaConsumerStart")
	defer span.End()

	// Subscribe to the topic, registering the rebalance callback
	err := c.kafkaConsumer.SubscribeTopics([]string{c.topic}, c.rebalanceCallback)
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

			if err := c.handleMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to handle message",
					"topic", *msg.TopicPartition.Topic,
					"partition", msg.TopicPartition.Partition,
					"offset", msg.TopicPartition.Offset,
					"error", err,
					"app", "mechanic-service")
				continue
			}

			// Commit Kafka offset only after the outbox write is durably committed
			if _, err := c.kafkaConsumer.CommitMessage(msg); err != nil {
				c.logger.Error("Failed to commit Kafka offset",
					"topic", *msg.TopicPartition.Topic,
					"partition", msg.TopicPartition.Partition,
					"offset", msg.TopicPartition.Offset,
					"error", err,
					"app", "mechanic-service")
				continue
			}

			c.logger.Info("Committed Kafka message and outbox event",
				"topic", *msg.TopicPartition.Topic,
				"partition", msg.TopicPartition.Partition,
				"offset", msg.TopicPartition.Offset,
				"app", "mechanic-service")
		}
	}
}

// handleMessage deserializes and processes a single Kafka message, writing
// it to the outbox inside a MongoDB transaction. The session is scoped to
// this function call, so EndSession fires on every message rather than only
// when Start() itself returns.
func (c *Consumer) handleMessage(ctx context.Context, msg *kafka.Message) error {
	// Track wall-clock time spent processing this single message. Since
	// ReadMessage(-1) in the Start() loop is the poll-equivalent call,
	// this is effectively the per-iteration contribution to the
	// poll-to-poll gap that max.poll.interval.ms measures. Logging it
	// gives real p95/p99 numbers to tune max.poll.interval.ms against,
	// instead of guessing, and the warning acts as an early-warning
	// canary before a rebalance actually happens.
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		c.logger.Info("Message processing time",
			"elapsed_ms", elapsed.Milliseconds(),
			"app", "mechanic-service")
		if elapsed > slowProcessingThreshold {
			c.logger.Warn("Slow message processing detected — approaching max.poll.interval.ms risks a rebalance",
				"elapsed_ms", elapsed.Milliseconds(),
				"threshold_ms", slowProcessingThreshold.Milliseconds(),
				"app", "mechanic-service")
		}
	}()

	ctx, span := c.tracer.Start(ctx, "ProcessKafkaMessage")
	defer span.End()

	// Deserialize Avro message
	if len(msg.Value) < 5 {
		err := fmt.Errorf("invalid message length")
		span.RecordError(err)
		span.SetStatus(codes.Error, "Invalid message length")
		c.logger.Error("Invalid message length", "length", len(msg.Value), "app", "mechanic-service")
		return err
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
			return fmt.Errorf("failed to fetch schema: %w", err)
		}
		c.schema, err = avro.Parse(schemaObj.Schema())
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to parse schema")
			c.logger.Error("Failed to parse schema", "schemaID", schemaID, "error", err, "app", "mechanic-service")
			return fmt.Errorf("failed to parse schema: %w", err)
		}
	}

	// Start a transaction to check and save the outbox event.
	// Session + EndSession are scoped to this call — fixes the leak where
	// the original code deferred EndSession inside the outer for-loop,
	// which meant it never actually ran until Start() itself returned.
	session, err := c.repo.GetMongoClient(ctx).StartSession()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to start MongoDB session")
		c.logger.Error("Failed to start MongoDB session", "error", err, "app", "mechanic-service")
		return fmt.Errorf("failed to start MongoDB session: %w", err)
	}
	defer session.EndSession(ctx)

	if err := session.StartTransaction(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to start transaction")
		c.logger.Error("Failed to start transaction", "error", err, "app", "mechanic-service")
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
		// Idempotency check: (topic, partition, offset) is the dedup key.
		// This is what actually prevents duplicate processing, independent
		// of Kafka offset commit timing.
		exists, err := c.repo.CheckOutboxEventExists(ctx, sc, *msg.TopicPartition.Topic, msg.TopicPartition.Partition, int64(msg.TopicPartition.Offset))
		if err != nil {
			c.logger.Error("Failed to check outbox event existence", "topic", *msg.TopicPartition.Topic, "partition", msg.TopicPartition.Partition, "offset", msg.TopicPartition.Offset, "error", err, "app", "mechanic-service")
			return fmt.Errorf("failed to check outbox event existence: %w", err)
		}
		if exists {
			c.logger.Info("Outbox event already exists, skipping", "topic", *msg.TopicPartition.Topic, "partition", msg.TopicPartition.Partition, "offset", msg.TopicPartition.Offset, "app", "mechanic-service")
			return nil
		}

		outboxEvent := &domain.OutboxEvent{
			ID:             primitive.NewObjectID().Hex(),
			EventType:      "RepairEvent",
			Payload:        msg.Value,
			CreatedAt:      time.Now(),
			Processed:      false,
			KafkaTopic:     *msg.TopicPartition.Topic,
			KafkaPartition: msg.TopicPartition.Partition,
			KafkaOffset:    int64(msg.TopicPartition.Offset),
		}
		if err := c.repo.SaveOutboxEvent(ctx, sc, outboxEvent); err != nil {
			return fmt.Errorf("failed to save outbox event: %w", err)
		}
		c.logger.Info("Saved outbox event in transaction", "eventID", outboxEvent.ID, "topic", outboxEvent.KafkaTopic, "partition", outboxEvent.KafkaPartition, "offset", outboxEvent.KafkaOffset, "app", "mechanic-service")
		return nil
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Transaction failed")
		c.logger.Error("Transaction failed", "error", err, "app", "mechanic-service")
		session.AbortTransaction(ctx)
		return err
	}

	if err := session.CommitTransaction(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to commit transaction")
		c.logger.Error("Failed to commit transaction", "error", err, "app", "mechanic-service")
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Close shuts down the Kafka consumer
func (c *Consumer) Close() {
	c.logger.Info("Closing Kafka consumer", "app", "mechanic-service")
	c.kafkaConsumer.Close()
}
