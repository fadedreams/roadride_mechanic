package main

import (
	"api-gateway/handlers"
	"api-gateway/logging"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
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
    // Step 1: Unauthenticated connection to initiate replica set (required in this Docker setup)
    initialURI := "mongodb://mongodb:27017/?directConnection=true"
    clientOptions := options.Client().ApplyURI(initialURI).SetConnectTimeout(10 * time.Second)
    client, err := mongo.Connect(context.Background(), clientOptions)
    if err != nil {
        slog.Error("failed to connect (initial unauth)", slog.String("error", err.Error()))
        return fmt.Errorf("failed to connect: %v", err)
    }
    defer client.Disconnect(context.Background())

    ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
    defer cancel()

    if err := client.Ping(ctx, nil); err != nil {
        slog.Error("failed to ping (initial unauth)", slog.String("error", err.Error()))
        return fmt.Errorf("failed to ping: %v", err)
    }
    slog.Info("Connected unauth for initiation")

    // Initiate (idempotent)
    adminDB := client.Database("admin")
    replSetConfig := bson.D{
        {Key: "replSetInitiate", Value: bson.D{
            {Key: "_id", Value: "rs0"},
            {Key: "members", Value: bson.A{
                bson.D{{Key: "_id", Value: 0}, {Key: "host", Value: "mongodb:27017"}},
            }},
        }},
    }
    _, err = adminDB.RunCommand(ctx, replSetConfig).DecodeBytes()
    if err != nil {
        if strings.Contains(err.Error(), "already initialized") {
            slog.Info("Replica set already initialized")
        } else {
            return fmt.Errorf("replSetInitiate failed: %v", err)
        }
    } else {
        slog.Info("Replica set initiated")
    }

    // Wait for PRIMARY
    for i := 0; i < 20; i++ {
        var status bson.M
        if err := adminDB.RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status); err == nil {
            if state, _ := status["myState"].(int32); state == 1 {
                slog.Info("MongoDB is PRIMARY")
                break
            }
        }
        if i == 19 {
            return fmt.Errorf("timeout waiting for PRIMARY")
        }
        time.Sleep(2 * time.Second)
    }

    // Step 2: Reconnect WITH auth for seeding
    finalURI := "mongodb://root:root@mongodb:27017/repairdb?replicaSet=rs0&authSource=admin"
    clientOptions = options.Client().ApplyURI(finalURI).SetConnectTimeout(10 * time.Second)
    client, err = mongo.Connect(context.Background(), clientOptions)
    if err != nil {
        return fmt.Errorf("failed to reconnect with auth: %v", err)
    }
    defer client.Disconnect(context.Background())

    if err := client.Ping(ctx, nil); err != nil {
        return fmt.Errorf("ping failed after auth reconnect: %v", err)
    }
    slog.Info("Reconnected with auth and replica set")

    // Now seed mechanics + index (same as before)
    mechanicsColl := client.Database("repairdb").Collection("mechanics")
    mechanics := []interface{}{
        bson.M{"_id": "mechanic1", "name": "Berlin Auto Repair", "location": bson.M{"longitude": 13.388860, "latitude": 52.517037}},
        bson.M{"_id": "mechanic2", "name": "City Garage", "location": bson.M{"longitude": 13.397634, "latitude": 52.529407}},
        bson.M{"_id": "mechanic3", "name": "Fast Fix Mechanics", "location": bson.M{"longitude": 13.428555, "latitude": 52.523219}},
    }
    mechanicsColl.DeleteMany(ctx, bson.M{}) // clear
    if _, err := mechanicsColl.InsertMany(ctx, mechanics); err != nil {
        return fmt.Errorf("insert mechanics failed: %v", err)
    }
    slog.Info("Seeded mechanics")

    outboxColl := client.Database("repairdb").Collection("mechanic_outbox")
    index := mongo.IndexModel{
        Keys: bson.D{{"kafka_topic", 1}, {"kafka_partition", 1}, {"kafka_offset", 1}},
        Options: options.Index().SetUnique(true),
    }
    if _, err := outboxColl.Indexes().CreateOne(ctx, index); err != nil {
        slog.Warn("Index create failed (maybe exists)", "error", err)
    } else {
        slog.Info("Created outbox index")
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
