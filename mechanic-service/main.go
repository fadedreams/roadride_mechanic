package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"log/slog"
	"mechanic-service/domain"
	"mechanic-service/handlers"
	"mechanic-service/logging"
	"mechanic-service/service"

	"github.com/gorilla/mux"
	"github.com/hashicorp/consul/api"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// initTracer initializes OpenTelemetry tracer
func initTracer(logger *slog.Logger) (func(), error) {
	jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "http://jaeger:4318/v1/traces"
	}
	logger.Info("Initializing tracer", "jaeger_endpoint", jaegerEndpoint, "app", "mechanic-service")

	// Create OTLP exporter
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint("jaeger:4318"),
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithURLPath("/v1/traces"),
	)
	if err != nil {
		logger.Error("Failed to create OTLP exporter", "error", err, "app", "mechanic-service")
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Test Jaeger connectivity
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://jaeger:16686/")
	if err != nil {
		logger.Error("Failed to connect to Jaeger UI (health check)", "error", err, "app", "mechanic-service")
	} else {
		logger.Info("Jaeger UI health check", "status_code", resp.StatusCode, "app", "mechanic-service")
		resp.Body.Close()
	}

	resources := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("mechanic-service"),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter, sdktrace.WithExportTimeout(5*time.Second))),
		sdktrace.WithResource(resources),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	// Force a test span
	ctx := context.Background()
	tr := otel.Tracer("mechanic-service")
	_, span := tr.Start(ctx, "TestSpan")
	span.SetAttributes(attribute.String("test", "true"))
	span.End()

	if err := tp.ForceFlush(ctx); err != nil {
		logger.Error("Failed to flush test span", "error", err, "app", "mechanic-service")
	} else {
		logger.Info("Test span flushed successfully", "app", "mechanic-service")
	}

	return func() {
		logger.Info("Shutting down tracer provider", "app", "mechanic-service")
		if err := tp.Shutdown(context.Background()); err != nil {
			logger.Error("Error shutting down tracer provider", "error", err, "app", "mechanic-service")
		}
	}, nil
}

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
	logger.Info("Starting mechanic-service", "app", "mechanic-service", "timestamp", time.Now().Unix())

	// Initialize tracer
	shutdown, err := initTracer(logger)
	if err != nil {
		logger.Error("Failed to initialize tracer", "error", err, "app", "mechanic-service")
		os.Exit(1)
	}
	defer shutdown()

	// Initialize Consul client and register service
	consulAddr := os.Getenv("CONSUL_ADDRESS")
	if consulAddr == "" {
		consulAddr = "consul:8500"
	}
	consulConfig := api.DefaultConfig()
	consulConfig.Address = consulAddr
	consulClient, err := api.NewClient(consulConfig)
	if err != nil {
		logger.Error("Failed to create Consul client", "error", err, "app", "mechanic-service")
		os.Exit(1)
	}

	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = "mechanic-service"
	}
	servicePort := os.Getenv("SERVICE_PORT")
	if servicePort == "" {
		servicePort = "8086"
	}
	serviceID := serviceName + "-" + servicePort
	registration := &api.AgentServiceRegistration{
		ID:      serviceID,
		Name:    serviceName,
		Port:    8086,
		Address: "mechanic-service",
		Check: &api.AgentServiceCheck{
			HTTP:     fmt.Sprintf("http://mechanic-service:%s/health", servicePort),
			Interval: "10s",
			Timeout:  "5s",
		},
	}
	if err := consulClient.Agent().ServiceRegister(registration); err != nil {
		logger.Error("Failed to register with Consul", "error", err, "app", "mechanic-service")
		os.Exit(1)
	}
	logger.Info("Registered with Consul", "service_id", serviceID, "app", "mechanic-service")

	// Initialize MongoDB
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://mongodb:27017/repairdb?replicaSet=rs0"
	}
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		logger.Error("Failed to connect to MongoDB", "error", err, "app", "mechanic-service")
		os.Exit(1)
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			logger.Error("Failed to disconnect from MongoDB", "error", err, "app", "mechanic-service")
		}
	}()
	logger.Info("Connected to MongoDB", "uri", mongoURI, "app", "mechanic-service")

	// Initialize repository and service
	repo := domain.NewMongoRepository(client)
	svc := service.NewService(repo, logger)

	// Initialize handler with service
	handler := handlers.NewMechanicHandler(svc, logger)

	// Initialize router
	r := mux.NewRouter()

	// Define endpoints
	r.HandleFunc("/health", handler.HealthCheck).Methods("GET")
	r.HandleFunc("/repairs/nearby", handler.ListNearbyRepairs).Methods("GET")
	r.HandleFunc("/repairs/{repairID}/assign", handler.AssignRepair).Methods("POST")

	// Start server
	logger.Info("Starting mechanic-service", "port", servicePort, "app", "mechanic-service")
	if err := http.ListenAndServe(":"+servicePort, r); err != nil {
		logger.Error("Failed to start server", "error", err, "app", "mechanic-service")
		os.Exit(1)
	}
}
