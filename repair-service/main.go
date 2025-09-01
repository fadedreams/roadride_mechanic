package main

import (
	"context"
	"encoding/json"
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
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// initTracer initializes OpenTelemetry tracer
func initTracer(logger *slog.Logger) (func(), error) {
	jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "http://jaeger:4318/v1/traces"
	}
	logger.Info("Initializing tracer", "jaeger_endpoint", jaegerEndpoint)

	// Create OTLP exporter
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint("jaeger:4318"),
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithURLPath("/v1/traces"),
	)
	if err != nil {
		logger.Error("Failed to create OTLP exporter", "error", err)
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Test Jaeger connectivity with a GET request to a health endpoint
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://jaeger:16686/")
	if err != nil {
		logger.Error("Failed to connect to Jaeger UI (health check)", "error", err)
	} else {
		logger.Info("Jaeger UI health check", "status_code", resp.StatusCode)
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

	return func() {
		logger.Info("Shutting down tracer provider")
		if err := tp.Shutdown(context.Background()); err != nil {
			logger.Error("Error shutting down tracer provider", "error", err)
		}
	}, nil
}

func connectToMongoDB(uri string, retries int, delay time.Duration, logger *slog.Logger) (*mongo.Client, error) {
	var client *mongo.Client
	var err error

	for i := range retries {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		client, err = mongo.Connect(ctx, options.Client().ApplyURI(uri))
		if err == nil {
			err = client.Ping(ctx, nil)
			if err == nil {
				// Verify replica set is initialized
				var result struct {
					Ok int `bson:"ok"`
				}
				err = client.Database("admin").RunCommand(ctx, bson.D{
					{Key: "replSetGetStatus", Value: 1},
				}).Decode(&result)
				if err == nil && result.Ok == 1 {
					cancel()
					logger.Info("Connected to MongoDB", "uri", uri)
					return client, nil
				}
				logger.Error("Replica set not ready", "error", err)
			}
		}
		cancel()
		logger.Error("Failed to connect to MongoDB", "attempt", i+1, "max_attempts", retries, "error", err)
		if i < retries-1 {
			time.Sleep(delay)
		}
	}
	return nil, fmt.Errorf("failed to connect to MongoDB after %d retries: %w", retries, err)
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

	// Initialize tracer
	shutdown, err := initTracer(logger)
	if err != nil {
		logger.Error("Failed to initialize tracer", "error", err)
		os.Exit(1)
	}
	defer shutdown()

	// Connect to MongoDB with retries
	client, err := connectToMongoDB("mongodb://mongodb:27017/repairdb?replicaSet=rs0", 5, 2*time.Second, logger)
	if err != nil {
		logger.Error("Failed to connect to MongoDB", "error", err)
		os.Exit(1)
	}

	// Initialize Consul client
	consulAddr := os.Getenv("CONSUL_ADDRESS")
	if consulAddr == "" {
		consulAddr = "consul:8500"
	}
	consulConfig := api.DefaultConfig()
	consulConfig.Address = consulAddr
	consulClient, err := api.NewClient(consulConfig)
	if err != nil {
		logger.Error("Failed to create Consul client", "error", err)
		os.Exit(1)
	}

	// Register service with Consul
	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = "repair-service"
	}
	servicePort := os.Getenv("SERVICE_PORT")
	if servicePort == "" {
		servicePort = "8083"
	}
	serviceID := serviceName + "-" + servicePort
	registration := &api.AgentServiceRegistration{
		ID:      serviceID,
		Name:    serviceName,
		Port:    8083,
		Address: "repair-service",
		Check: &api.AgentServiceCheck{
			HTTP:     fmt.Sprintf("http://repair-service:%s/health", servicePort),
			Interval: "10s",
			Timeout:  "5s",
		},
	}
	if err := consulClient.Agent().ServiceRegister(registration); err != nil {
		logger.Error("Failed to register with Consul", "error", err)
		os.Exit(1)
	}

	// Initialize repository and service
	repo := domain.NewMongoRepository(client)
	svc := service.NewService(repo, logger)

	// Initialize router
	r := mux.NewRouter()
	r.Use(otelmux.Middleware("repair-service"))

	// Health check endpoint for Consul
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, span := otel.Tracer("repair-service").Start(r.Context(), "HealthCheck")
		defer span.End()
		logger.Info("Health check requested")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	}).Methods("GET")

	// Create repair endpoint
	r.HandleFunc("/repairs", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("repair-service").Start(r.Context(), "CreateRepair")
		defer span.End()

		logger.Info("Received POST /repairs request")
		var cost domain.RepairCostModel
		if err := json.NewDecoder(r.Body).Decode(&cost); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Invalid request body")
			logger.Error("Failed to decode request body", "error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid request body: " + err.Error()})
			return
		}
		logger.Info("Decoded cost", "cost", cost)
		span.SetAttributes(
			attribute.String("userID", cost.UserID),
			attribute.String("repairType", cost.RepairType),
			attribute.Float64("totalPrice", cost.TotalPrice),
		)
		if cost.ID == "" {
			cost.ID = primitive.NewObjectID().Hex()
			logger.Info("Generated new ID for cost", "costID", cost.ID)
			span.SetAttributes(attribute.String("costID", cost.ID))
		}
		repair, err := svc.CreateRepair(ctx, &cost)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to create repair")
			logger.Error("Failed to create repair", "error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "Failed to create repair: " + err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(repair); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to encode response")
			logger.Error("Failed to encode response", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "Failed to encode response: " + err.Error()})
			return
		}
		logger.Info("Successfully sent response for POST /repairs")
	}).Methods("POST")

	// Estimate repair cost endpoint
	r.HandleFunc("/repairs/estimate", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("repair-service").Start(r.Context(), "EstimateRepairCost")
		defer span.End()

		var input struct {
			RepairType string          `json:"repairType"`
			UserID     string          `json:"userID"`
			Location   domain.Location `json:"location"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Invalid request body")
			logger.Error("Failed to decode request body", "error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid request body: " + err.Error()})
			return
		}
		span.SetAttributes(
			attribute.String("repairType", input.RepairType),
			attribute.String("userID", input.UserID),
			attribute.Float64("location.longitude", input.Location.Longitude),
			attribute.Float64("location.latitude", input.Location.Latitude),
		)
		cost, err := svc.EstimateRepairCost(ctx, input.RepairType, input.UserID, &input.Location)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to estimate repair cost")
			logger.Error("Failed to estimate repair cost", "error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Failed to estimate repair cost: " + err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(cost); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to encode response")
			logger.Error("Failed to encode response", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "Failed to encode response: " + err.Error()})
			return
		}
	}).Methods("POST")

	// Get all repairs endpoint
	r.HandleFunc("/repairs", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("repair-service").Start(r.Context(), "GetAllRepairs")
		defer span.End()

		logger.Info("Received GET /repairs request")
		repairs, err := svc.GetAllRepairs(ctx)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to get repairs")
			logger.Error("Failed to get repairs", "error", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Failed to get repairs: " + err.Error()})
			return
		}
		span.SetAttributes(
			attribute.Int("repairCount", len(repairs)),
		)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(repairs); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to encode response")
			logger.Error("Failed to encode response", "error", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "Failed to encode response: " + err.Error()})
			return
		}
		logger.Info("Successfully sent response for GET /repairs")
	}).Methods("GET")

	// Start gRPC server in a separate goroutine
	go func() {
		grpcPort := os.Getenv("GRPC_PORT")
		if grpcPort == "" {
			grpcPort = "50051"
		}
		lis, err := net.Listen("tcp", ":"+grpcPort)
		if err != nil {
			logger.Error("Failed to listen for gRPC", "error", err)
			os.Exit(1)
		}
		grpcServer := grpc.NewServer()
		proto.RegisterRepairServiceServer(grpcServer, grpcsvc.NewRepairServer(repo, logger))
		reflection.Register(grpcServer) // Enable reflection for debugging
		logger.Info("Starting gRPC server", "port", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("Failed to start gRPC server", "error", err)
			os.Exit(1)
		}
	}()

	// Start server
	port := os.Getenv("SERVICE_PORT")
	if port == "" {
		port = "8083"
	}
	logger.Info("Starting repair-service", "port", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		logger.Error("Failed to start server", "error", err)
		os.Exit(1)
	}
}
