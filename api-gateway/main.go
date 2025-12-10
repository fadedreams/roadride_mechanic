package main

import (
	"api-gateway/handlers"
	"api-gateway/logging"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func main() {
	// Initialize structured logging
	logger, logFile, err := logging.NewLogger()
	if err != nil {
		slog.Error("Failed to initialize logger", "error", err)
		os.Exit(1)
	}
	defer logFile.Close()
	slog.SetDefault(logger)

	// Log startup
	slog.Info("Starting API Gateway", "app", "api-gateway", "timestamp", time.Now().Unix())

	// Initialize MongoDB
	if err := initMongoDB(); err != nil {
		slog.Error("Failed to initialize MongoDB", "error", err)
		os.Exit(1)
	}

	// Initialize tracer
	shutdown, err := initTracer()
	if err != nil {
		slog.Error("Failed to initialize tracer", "error", err)
		os.Exit(1)
	}
	defer shutdown()

	// Initialize handler
	repairHandler := handlers.NewRepairHandler()

	// Initialize router
	r := mux.NewRouter()

	// Add OpenTelemetry middleware
	r.Use(otelmux.Middleware("api-gateway"))

	// Define endpoints
	r.HandleFunc("/health", repairHandler.HealthCheck).Methods("GET")
	r.HandleFunc("/repairs", repairHandler.CreateRepair).Methods("POST")
	r.HandleFunc("/repairs/estimate", repairHandler.EstimateRepairCost).Methods("POST")
	r.HandleFunc("/repairs/nearby", repairHandler.ListNearbyRepairs).Methods("GET")
	r.HandleFunc("/repairs/cost/{costID}", repairHandler.GetRepairCost).Methods("GET")
	r.HandleFunc("/repairs/{repairID}", repairHandler.GetRepair).Methods("GET")
	r.HandleFunc("/repairs/{repairID}", repairHandler.UpdateRepair).Methods("PUT")
	r.HandleFunc("/ws", repairHandler.HandleWebSocket).Methods("GET")

	// Start server
	slog.Info("API Gateway running on port 8085")
	if err := http.ListenAndServe(":8085", r); err != nil {
		slog.Error("Failed to start server", "error", err)
		os.Exit(1)
	}
}

func initMongoDB() error {
	// Correct connection string for Bitnami MongoDB replica set (works with 1, 2, 3+ members)
	uri := "mongodb://root:password@mongodb-headless.default.svc.cluster.local:27017/repairdb?replicaSet=rs0&authSource=admin"

	clientOptions := options.Client().
		ApplyURI(uri).
		SetConnectTimeout(10*time.Second).
		SetServerSelectionTimeout(10*time.Second).
		SetHeartbeatInterval(10*time.Second).
		SetRetryWrites(true).
		SetRetryReads(true)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to create MongoDB client: %w", err)
	}

	// Verify connection — driver automatically discovers the PRIMARY
	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB replica set: %w", err)
	}

	slog.Info("Successfully connected to MongoDB replica set (rs0)")

	// Optional: store client globally if other packages need it
	// mongoClient = client

	// === Seed initial data (idempotent) ===
	mechanicsColl := client.Database("repairdb").Collection("mechanics")

	// Drop and re-insert mechanics (safe for dev/local, remove in prod if unwanted)
	if _, err := mechanicsColl.DeleteMany(ctx, bson.M{}); err != nil {
		slog.Warn("Failed to clear mechanics collection", "error", err)
	}

	mechanics := []interface{}{
		bson.M{
			"_id": "mechanic1",
			"name": "Berlin Auto Repair",
			"location": bson.M{"longitude": 13.388860, "latitude": 52.517037},
		},
		bson.M{
			"_id": "mechanic2",
			"name": "City Garage",
			"location": bson.M{"longitude": 13.397634, "latitude": 52.529407},
		},
		bson.M{
			"_id": "mechanic3",
			"name": "Fast Fix Mechanics",
			"location": bson.M{"longitude": 13.428555, "latitude": 52.523219},
		},
	}

	if _, err := mechanicsColl.InsertMany(ctx, mechanics); err != nil {
		return fmt.Errorf("failed to insert mechanics: %w", err)
	}
	slog.Info("Inserted 3 mechanics into repairdb.mechanics")

	// Create unique index on outbox collection (idempotent)
	outboxColl := client.Database("repairdb").Collection("mechanic_outbox")
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "kafka_topic", Value: 1},
			{Key: "kafka_partition", Value: 1},
			{Key: "kafka_offset", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}

	_, err = outboxColl.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		// Ignore if index already exists
		if !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("failed to create unique index on mechanic_outbox: %w", err)
		}
		slog.Info("Unique index on mechanic_outbox already exists")
	} else {
		slog.Info("Created unique index on mechanic_outbox")
	}

	return nil
}

func initTracer() (func(), error) {
	jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "http://jaeger:4318/v1/traces"
	}
	slog.Info("Initializing tracer", "jaeger_endpoint", jaegerEndpoint)

	// Create OTLP exporter
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint("jaeger:4318"),
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithURLPath("/v1/traces"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %v", err)
	}

	// Test Jaeger connectivity with a GET request to the UI health endpoint
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://jaeger:16686/")
	if err != nil {
		slog.Error("Failed to connect to Jaeger UI (health check)", "error", err)
	} else {
		slog.Info("Jaeger UI health check", "status_code", resp.StatusCode)
		resp.Body.Close()
	}

	resources := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("api-gateway"),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter, sdktrace.WithExportTimeout(5*time.Second))),
		sdktrace.WithResource(resources),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	// Force a test span to verify export
	ctx := context.Background()
	tr := otel.Tracer("api-gateway")
	_, span := tr.Start(ctx, "TestSpan")
	span.SetAttributes(attribute.String("test", "true"))
	span.End()

	// Force export
	if err := tp.ForceFlush(ctx); err != nil {
		slog.Error("Failed to flush test span", "error", err)
	} else {
		slog.Info("Test span flushed successfully")
	}

	return func() {
		slog.Info("Shutting down tracer provider")
		if err := tp.Shutdown(context.Background()); err != nil {
			slog.Error("Error shutting down tracer provider", "error", err)
		}
	}, nil
}
