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
	// Set up MongoDB client options with directConnection=true for uninitialized replica set
	clientOptions := options.Client().
		ApplyURI("mongodb://mongodb:27017/?directConnection=true").
		SetConnectTimeout(10 * time.Second)

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		slog.Error("failed to connect to MongoDB", slog.String("error", err.Error()))
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background())

	// Ping the MongoDB server to verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx, nil); err != nil {
		slog.Error("failed to ping MongoDB", slog.String("error", err.Error()))
		return fmt.Errorf("failed to ping MongoDB: %v", err)
	}
	slog.Info("Connected to MongoDB")

	// Initialize the replica set
	adminDB := client.Database("admin")
	replSetConfig := bson.D{
		{Key: "replSetInitiate", Value: bson.D{
			{Key: "_id", Value: "rs0"},
			{Key: "members", Value: bson.A{
				bson.D{
					{Key: "_id", Value: 0},
					{Key: "host", Value: "mongodb:27017"},
				},
			}},
		}},
	}

	result, err := adminDB.RunCommand(ctx, replSetConfig).DecodeBytes()
	if err != nil {
		if err.Error() == "command replSetInitiate failed: already initialized" {
			slog.Info("Replica set already initialized (safe to ignore).")
		} else {
			slog.Error("failed to initialize replica set", slog.String("error", err.Error()))
			return fmt.Errorf("failed to initialize replica set: %v", err)
		}
	} else {
		slog.Info("Replica set initialized successfully", "result", result.String())
	}

	// Optional: Quick check for PRIMARY state (single check or short loop; safe to skip for single-member sets)
	// For single-member sets, this should pass immediately after initiate.
	statusCtx, statusCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer statusCancel()
	var statusDoc bson.M
	if err := adminDB.RunCommand(statusCtx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&statusDoc); err != nil {
		slog.Error("failed to get replica set status", slog.String("error", err.Error()))
		return fmt.Errorf("failed to get replica set status: %v", err)
	}
	slog.Info("Replica set status", "status", fmt.Sprintf("%+v", statusDoc))

	// Check myState with flexible type handling (could be int32, int64, or float64 in BSON)
	myStateVal, ok := statusDoc["myState"]
	if !ok {
		return fmt.Errorf("myState not found in replica set status")
	}
	myStateNum, ok := myStateVal.(int32)
	if !ok {
		if myStateNum64, ok64 := myStateVal.(int64); ok64 {
			myStateNum = int32(myStateNum64)
		} else if myStateFloat, okFloat := myStateVal.(float64); okFloat {
			myStateNum = int32(myStateFloat)
		} else {
			return fmt.Errorf("myState has unexpected type: %T (value: %v)", myStateVal, myStateVal)
		}
	}
	if myStateNum != 1 {
		// Optional short loop if not PRIMARY (rare for single-member)
		for i := 0; i < 10; i++ {  // Reduced to 20 seconds max
			statusCtx, statusCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer statusCancel()
			if err := adminDB.RunCommand(statusCtx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&statusDoc); err != nil {
				slog.Error("failed to get replica set status in loop", slog.String("error", err.Error()))
				return fmt.Errorf("failed to get replica set status in loop: %v", err)
			}
			myStateVal, _ = statusDoc["myState"]
			if myStateNum64, ok64 := myStateVal.(int64); ok64 && myStateNum64 == 1 {
				slog.Info("Replica set is now in PRIMARY state")
				break
			} else if myStateFloat, okFloat := myStateVal.(float64); okFloat && myStateFloat == 1 {
				slog.Info("Replica set is now in PRIMARY state")
				break
			}
			slog.Info("Waiting for replica set to become PRIMARY", "attempt", i+1)
			time.Sleep(2 * time.Second)
		}
		if myStateNum != 1 {
			return fmt.Errorf("replica set did not become PRIMARY after waiting")
		}
	} else {
		slog.Info("Replica set is already in PRIMARY state")
	}

	// Reconnect with replica set URI (MongoDB driver will handle primary discovery)
	clientOptions = options.Client().
		ApplyURI("mongodb://mongodb:27017/repairdb?replicaSet=rs0").
		SetConnectTimeout(10 * time.Second)
	client, err = mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		slog.Error("failed to reconnect to MongoDB with replica set", slog.String("error", err.Error()))
		return fmt.Errorf("failed to reconnect to MongoDB with replica set: %v", err)
	}
	defer client.Disconnect(context.Background())

	// Initialize mechanics collection
	mechanicsColl := client.Database("repairdb").Collection("mechanics")
	mechanics := []interface{}{
		bson.M{
			"_id": "mechanic1",
			"name": "Berlin Auto Repair",
			"location": bson.M{
				"longitude": 13.388860,
				"latitude":  52.517037,
			},
		},
		bson.M{
			"_id": "mechanic2",
			"name": "City Garage",
			"location": bson.M{
				"longitude": 13.397634,
				"latitude":  52.529407,
			},
		},
		bson.M{
			"_id": "mechanic3",
			"name": "Fast Fix Mechanics",
			"location": bson.M{
				"longitude": 13.428555,
				"latitude":  52.523219,
			},
		},
	}

	// Drop and insert mechanics (idempotent)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := mechanicsColl.Drop(ctx); err != nil {
		slog.Warn("Failed to drop mechanics collection (may not exist)", "error", err)
	}
	_, err = mechanicsColl.InsertMany(ctx, mechanics)
	if err != nil {
		slog.Error("failed to insert mechanics", slog.String("error", err.Error()))
		return fmt.Errorf("failed to insert mechanics: %v", err)
	}
	slog.Info("Inserted mechanics data successfully")

	// Create index on mechanic_outbox
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
		slog.Error("failed to create index on mechanic_outbox", slog.String("error", err.Error()))
		return fmt.Errorf("failed to create index on mechanic_outbox: %v", err)
	}
	slog.Info("Created index on mechanic_outbox successfully")

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
