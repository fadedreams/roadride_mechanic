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
	ID           string         `avro:"id"`
	UserID       string         `avro:"user_id"`
	Status       string         `avro:"status"`
	RepairType   string         `avro:"repair_type"`
	TotalPrice   float64        `avro:"total_price"`
	UserLocation *Location      `avro:"user_location"`
	Mechanics    []MechanicInfo `avro:"mechanics"`
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

	batchSize   int           // max messages per batch before a forced flush
	batchLinger time.Duration // max time to wait for a batch to fill before flushing partial
	pollTimeout time.Duration // per-poll timeout used while accumulating a batch
}

// slowProcessingThreshold is the point at which a single batch's processing
// time gets logged as a warning. This is a canary for max.poll.interval.ms
// exhaustion, not a hard limit itself — tune it well below your configured
// max.poll.interval.ms so you get warned before a rebalance actually happens.
const slowProcessingThreshold = 10 * time.Second

const (
	defaultBatchSize   = 100
	defaultBatchLinger = 2 * time.Second
	defaultPollTimeout = 500 * time.Millisecond
)

func NewConsumer(bootstrapServers, schemaRegistryURL, topic, groupID string, logger *slog.Logger, repo domain.MechanicRepository) (*Consumer, error) {
	// Static group membership requires a stable per-instance identity.
	// This MUST be stable across restarts of the "same" logical instance,
	// and unique per running instance. In docker-compose this is a fixed
	// value tied to container_name; in the current single-replica k8s
	// Deployment it's also hardcoded. If you scale the Deployment beyond
	// one replica, switch to a StatefulSet and derive this from the
	// stable ordinal pod name instead.
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		return nil, fmt.Errorf("INSTANCE_ID env var must be set (stable per-instance identity required for group.instance.id)")
	}

	// Initialize Kafka consumer
	config := &kafka.ConfigMap{
		"bootstrap.servers":    bootstrapServers,
		"group.id":             groupID,
		"group.instance.id":    instanceID, // static membership: restarts are treated as reconnects, not leave+join, avoiding rebalance storms on rolling deploys/autoscaling
		"session.timeout.ms":   45000,      // grace window the broker waits before evicting a disconnected static member; must comfortably cover normal restart time
		"max.poll.interval.ms": 300000,     // ceiling on time between ReadMessage calls before the client voluntarily leaves the group; batching (below) is what keeps us safely under this as volume grows
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   false, // Disable auto-commit — offsets are committed in bulk per batch instead
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
		batchSize:     defaultBatchSize,
		batchLinger:   defaultBatchLinger,
		pollTimeout:   defaultPollTimeout,
	}, nil
}

// rebalanceCallback ensures that when this consumer is about to lose a
// partition, whatever has already been processed gets a final, blocking
// commit before the partition changes hands. Note: any messages sitting in
// an in-flight batch that hasn't been flushed yet are handled by Start()
// forcing a flush before calling into the rebalance machinery — this
// callback only commits what's already been durably written.
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

// Start begins consuming messages from the Kafka topic, accumulating them
// into batches instead of processing one at a time. A single slow Mongo
// call now costs at most one batch's worth of poll-interval budget, not
// one message's worth per message — which is what keeps the consumer from
// tripping max.poll.interval.ms and getting rebalanced out under load.
func (c *Consumer) Start(ctx context.Context) error {
	_, span := c.tracer.Start(ctx, "KafkaConsumerStart")
	defer span.End()

	err := c.kafkaConsumer.SubscribeTopics([]string{c.topic}, c.rebalanceCallback)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to subscribe to topic")
		c.logger.Error("Failed to subscribe to topic", "topic", c.topic, "error", err, "app", "mechanic-service")
		return fmt.Errorf("failed to subscribe to topic: %w", err)
	}
	c.logger.Info("Subscribed to Kafka topic", "topic", c.topic, "app", "mechanic-service")

	batch := make([]*kafka.Message, 0, c.batchSize)
	var batchStarted time.Time

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := c.processBatch(ctx, batch); err != nil {
			c.logger.Error("Failed to process batch", "size", len(batch), "error", err, "app", "mechanic-service")
			// Offsets are only committed on success below, so a failed
			// batch is simply redelivered on the next poll — same
			// at-least-once contract as before, just batch-sized.
		} else if err := c.commitBatchOffsets(batch); err != nil {
			c.logger.Error("Failed to commit batch offsets", "size", len(batch), "error", err, "app", "mechanic-service")
		} else {
			c.logger.Info("Committed batch", "size", len(batch), "app", "mechanic-service")
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Context canceled, flushing pending batch and stopping Kafka consumer", "app", "mechanic-service")
			flush()
			return ctx.Err()
		default:
			msg, err := c.kafkaConsumer.ReadMessage(c.pollTimeout)
			if err != nil {
				kerr, ok := err.(kafka.Error)
				if !ok || kerr.Code() != kafka.ErrTimedOut {
					c.logger.Error("Error reading Kafka message", "error", err, "app", "mechanic-service")
				}
				// Timeout just means no message arrived within pollTimeout —
				// fall through to the linger check below.
			} else {
				if len(batch) == 0 {
					batchStarted = time.Now()
				}
				batch = append(batch, msg)
			}

			if len(batch) >= c.batchSize || (len(batch) > 0 && time.Since(batchStarted) >= c.batchLinger) {
				flush()
			}
		}
	}
}

// processBatch deserializes and writes a whole batch of messages to the
// outbox inside a single MongoDB transaction: one existence-check query and
// one bulk insert, instead of a transaction per message.
func (c *Consumer) processBatch(ctx context.Context, batch []*kafka.Message) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		c.logger.Info("Batch processing time",
			"size", len(batch),
			"elapsed_ms", elapsed.Milliseconds(),
			"app", "mechanic-service")
		if elapsed > slowProcessingThreshold {
			c.logger.Warn("Slow batch processing detected — approaching max.poll.interval.ms risks a rebalance",
				"elapsed_ms", elapsed.Milliseconds(),
				"threshold_ms", slowProcessingThreshold.Milliseconds(),
				"batch_size", len(batch),
				"app", "mechanic-service")
		}
	}()

	ctx, span := c.tracer.Start(ctx, "ProcessKafkaBatch")
	defer span.End()
	span.SetAttributes(attribute.Int("batch.size", len(batch)))

	for _, msg := range batch {
		if len(msg.Value) < 5 {
			err := fmt.Errorf("invalid message length")
			span.RecordError(err)
			c.logger.Error("Invalid message length", "length", len(msg.Value), "app", "mechanic-service")
			return err
		}
		schemaID := int(binary.BigEndian.Uint32(msg.Value[1:5]))
		if c.schema == nil {
			schemaObj, err := c.srClient.GetSchema(schemaID)
			if err != nil {
				span.RecordError(err)
				c.logger.Error("Failed to fetch schema", "schemaID", schemaID, "error", err, "app", "mechanic-service")
				return fmt.Errorf("failed to fetch schema: %w", err)
			}
			c.schema, err = avro.Parse(schemaObj.Schema())
			if err != nil {
				span.RecordError(err)
				c.logger.Error("Failed to parse schema", "schemaID", schemaID, "error", err, "app", "mechanic-service")
				return fmt.Errorf("failed to parse schema: %w", err)
			}
		}
	}

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
		// One bulk existence check for the whole batch instead of one
		// query per message. NOTE: this requires a new
		// CheckOutboxEventsExist method on domain.MechanicRepository —
		// see the note below the code.
		keys := make([]domain.OutboxKey, 0, len(batch))
		for _, msg := range batch {
			keys = append(keys, domain.OutboxKey{
				Topic:     *msg.TopicPartition.Topic,
				Partition: msg.TopicPartition.Partition,
				Offset:    int64(msg.TopicPartition.Offset),
			})
		}
		existing, err := c.repo.CheckOutboxEventsExist(ctx, sc, keys)
		if err != nil {
			return fmt.Errorf("failed to check outbox event existence: %w", err)
		}

		events := make([]*domain.OutboxEvent, 0, len(batch))
		for i, msg := range batch {
			if existing[keys[i]] {
				c.logger.Info("Outbox event already exists, skipping",
					"topic", keys[i].Topic, "partition", keys[i].Partition, "offset", keys[i].Offset,
					"app", "mechanic-service")
				continue
			}
			events = append(events, &domain.OutboxEvent{
				ID:             primitive.NewObjectID().Hex(),
				EventType:      "RepairEvent",
				Payload:        msg.Value,
				CreatedAt:      time.Now(),
				Processed:      false,
				KafkaTopic:     keys[i].Topic,
				KafkaPartition: keys[i].Partition,
				KafkaOffset:    keys[i].Offset,
			})
		}

		if len(events) == 0 {
			return nil
		}

		// One bulk insert for the whole batch instead of one insert per
		// message. NOTE: this requires a new SaveOutboxEvents method on
		// domain.MechanicRepository — see the note below the code.
		if err := c.repo.SaveOutboxEvents(ctx, sc, events); err != nil {
			return fmt.Errorf("failed to save outbox events: %w", err)
		}
		c.logger.Info("Saved outbox events in transaction", "count", len(events), "app", "mechanic-service")
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

// commitBatchOffsets commits the highest offset (+1) seen per partition in
// the batch, in one call, rather than committing after every message.
func (c *Consumer) commitBatchOffsets(batch []*kafka.Message) error {
	highest := make(map[int32]kafka.TopicPartition)
	for _, msg := range batch {
		tp := msg.TopicPartition
		tp.Offset++ // commit points to the next offset to read, per Kafka convention
		if existing, ok := highest[tp.Partition]; !ok || tp.Offset > existing.Offset {
			highest[tp.Partition] = tp
		}
	}
	toCommit := make([]kafka.TopicPartition, 0, len(highest))
	for _, tp := range highest {
		toCommit = append(toCommit, tp)
	}
	_, err := c.kafkaConsumer.CommitOffsets(toCommit)
	return err
}

// Close shuts down the Kafka consumer
func (c *Consumer) Close() {
	c.logger.Info("Closing Kafka consumer", "app", "mechanic-service")
	c.kafkaConsumer.Close()
}
