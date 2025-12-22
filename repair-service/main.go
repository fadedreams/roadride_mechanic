
package main

import (
	"context"
	// "encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"repair-service/domain"
	"repair-service/grpcsvc"
	"repair-service/logging"
	"repair-service/proto"
	"repair-service/service"

	"log/slog"

	"github.com/gorilla/mux"
	"github.com/hashicorp/consul/api"
	// "go.mongodb.org/mongo-driver/bson"
	// "go.mongodb.org/mongo-driver/bson/primitive"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func initTracer() (func(), error) {
	jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "http://jaeger:4318/v1/traces"
	}
	slog.Info("Initializing tracer", "jaeger_endpoint", jaegerEndpoint)

	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint("jaeger:4318"),
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithURLPath("/v1/traces"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %v", err)
	}

	// Test Jaeger connectivity
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
		semconv.ServiceNameKey.String("repair-service"),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter, sdktrace.WithExportTimeout(5*time.Second))),
		sdktrace.WithResource(resources),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	// Force a test span to verify export
	ctx := context.Background()
	tr := otel.Tracer("repair-service")
	_, span := tr.Start(ctx, "TestSpan")
	span.SetAttributes(attribute.String("test", "true"))
	span.End()
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

func initMongoDB() (*mongo.Client, error) {
	uri := "mongodb://root:password@mongodb-headless.default.svc.cluster.local:27017/repairdb?replicaSet=rs0&authSource=admin"

	clientOptions := options.Client().
		ApplyURI(uri).
		SetConnectTimeout(10 * time.Second).
		SetServerSelectionTimeout(10 * time.Second).
		SetHeartbeatInterval(10 * time.Second).
		SetRetryWrites(true).
		SetRetryReads(true)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create MongoDB client: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB replica set: %w", err)
	}

	slog.Info("Successfully connected to MongoDB replica set (rs0)")
	return client, nil
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
	logger.Info("Starting repair-service", "app", "repair-service", "timestamp", time.Now().Unix())

	// Initialize Consul client and register service
	consulAddr := os.Getenv("CONSUL_ADDRESS")
	if consulAddr == "" {
		consulAddr = "consul:8500"
	}
	consulConfig := api.DefaultConfig()
	consulConfig.Address = consulAddr
	consulClient, err := api.NewClient(consulConfig)
	if err != nil {
		logger.Error("Failed to create Consul client", "error", err, "app", "repair-service")
		os.Exit(1)
	}

	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = "repair-service"
	}
	servicePort := os.Getenv("SERVICE_PORT")
	if servicePort == "" {
		servicePort = "8087"
	}
	serviceID := serviceName + "-" + servicePort

	registration := &api.AgentServiceRegistration{
		ID:      serviceID,
		Name:    serviceName,
		Port:    8087,
		Address: "repair-service",
		Check: &api.AgentServiceCheck{
			HTTP:     "http://repair-service:8087/health",
			Interval: "10s",
			Timeout:  "5s",
		},
	}
	if err := consulClient.Agent().ServiceRegister(registration); err != nil {
		logger.Error("Failed to register with Consul", "error", err, "app", "repair-service")
		os.Exit(1)
	}
	logger.Info("Registered with Consul", "serviceID", serviceID, "app", "repair-service")

	// Initialize tracer
	shutdown, err := initTracer()
	if err != nil {
		logger.Error("Failed to initialize tracer", "error", err, "app", "repair-service")
		os.Exit(1)
	}
	defer shutdown()

	// Connect to MongoDB
	client, err := initMongoDB()
	if err != nil {
		logger.Error("Failed to initialize MongoDB", "error", err, "app", "repair-service")
		os.Exit(1)
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			logger.Error("Failed to disconnect from MongoDB", "error", err, "app", "repair-service")
		}
	}()

	// Initialize repository and service
	repo := domain.NewMongoRepository(client)
	// svc := service.NewService(repo, logger)

	// Initialize router
	r := mux.NewRouter()
	r.Use(otelmux.Middleware("repair-service"))

	// Health check endpoint for Consul
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, span := otel.Tracer("repair-service").Start(r.Context(), "HealthCheck")
		defer span.End()
		logger.Info("Health check requested", "app", "repair-service")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	}).Methods("GET")

	// Your existing endpoints (unchanged)
	r.HandleFunc("/repairs", func(w http.ResponseWriter, r *http.Request) {
		// ... (keep your existing CreateRepair handler code)
	}).Methods("POST")

	r.HandleFunc("/repairs/estimate", func(w http.ResponseWriter, r *http.Request) {
		// ... (keep your existing EstimateRepairCost handler code)
	}).Methods("POST")

	r.HandleFunc("/repairs", func(w http.ResponseWriter, r *http.Request) {
		// ... (keep your existing GetAllRepairs handler code)
	}).Methods("GET")

	// Start gRPC server in a separate goroutine
	go func() {
		grpcPort := os.Getenv("GRPC_PORT")
		if grpcPort == "" {
			grpcPort = "50051"
		}
		lis, err := net.Listen("tcp", ":"+grpcPort)
		if err != nil {
			logger.Error("Failed to listen for gRPC", "error", err, "app", "repair-service")
			os.Exit(1)
		}
		grpcServer := grpc.NewServer()
		proto.RegisterRepairServiceServer(grpcServer, grpcsvc.NewRepairServer(repo, logger))
		reflection.Register(grpcServer)
		logger.Info("Starting gRPC server", "port", grpcPort, "app", "repair-service")
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("Failed to start gRPC server", "error", err, "app", "repair-service")
			os.Exit(1)
		}
	}()

	// Start HTTP server
	port := os.Getenv("SERVICE_PORT")
	if port == "" {
		port = "8087"
	}
	logger.Info("API Gateway running on port " + port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		logger.Error("Failed to start server", "error", err, "app", "repair-service")
		os.Exit(1)
	}
}


