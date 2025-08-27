package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"repair-service/domain"
	"repair-service/service"
	"time"

	"net"
	"repair-service/grpcsvc"
	"repair-service/proto"

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
func initTracer() (func(), error) {
	jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "http://jaeger:4318/v1/traces"
	}
	log.Printf("Initializing tracer with Jaeger endpoint: %s", jaegerEndpoint)

	// Create OTLP exporter
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint("jaeger:4318"),
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithURLPath("/v1/traces"),
	)
	if err != nil {
		log.Printf("Failed to create OTLP exporter: %v", err)
		return nil, fmt.Errorf("failed to create OTLP exporter: %v", err)
	}

	// Test Jaeger connectivity with a GET request to a health endpoint
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://jaeger:16686/")
	if err != nil {
		log.Printf("Failed to connect to Jaeger UI (health check): %v", err)
	} else {
		log.Printf("Jaeger UI health check: status %d", resp.StatusCode)
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
		log.Printf("Shutting down tracer provider")
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}, nil
}

func connectToMongoDB(uri string, retries int, delay time.Duration) (*mongo.Client, error) {
    var client *mongo.Client
    var err error

    for i := 0; i < retries; i++ {
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        client, err = mongo.Connect(ctx, options.Client().ApplyURI(uri))
        if err == nil {
            err = client.Ping(ctx, nil)
            if err == nil {
                // Verify replica set is initialized
                var result struct {
                    Ok int `bson:"ok"`
                }
                err = client.Database("admin").RunCommand(ctx, bson.D{{"replSetGetStatus", 1}}).Decode(&result)
                if err == nil && result.Ok == 1 {
                    cancel()
                    return client, nil
                }
                log.Printf("Replica set not ready: %v", err)
            }
        }
        cancel()
        log.Printf("Failed to connect to MongoDB (attempt %d/%d): %v", i+1, retries, err)
        if i < retries-1 {
            time.Sleep(delay)
        }
    }
    return nil, fmt.Errorf("failed to connect to MongoDB after %d retries: %v", retries, err)
}

func main() {
	// Initialize tracer
	shutdown, err := initTracer()
	if err != nil {
		log.Fatalf("Failed to initialize tracer: %v", err)
	}
	defer shutdown()

	// Connect to MongoDB with retries
	client, err := connectToMongoDB("mongodb://admin:admin@mongodb:27017", 5, 2*time.Second)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
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
		log.Fatalf("Failed to create Consul client: %v", err)
	}

	// Register service with Consul
	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = "repair-service"
	}
	servicePort := os.Getenv("SERVICE_PORT")
	if servicePort == "" {
		servicePort = "8080"
	}
	serviceID := serviceName + "-" + servicePort
	registration := &api.AgentServiceRegistration{
		ID:      serviceID,
		Name:    serviceName,
		Port:    8080,
		Address: "repair-service",
		Check: &api.AgentServiceCheck{
			HTTP:     fmt.Sprintf("http://repair-service:8080/health"),
			Interval: "10s",
			Timeout:  "5s",
		},
	}
	if err := consulClient.Agent().ServiceRegister(registration); err != nil {
		log.Fatalf("Failed to register with Consul: %v", err)
	}

	// Initialize repository and service
	repo := domain.NewMongoRepository(client)
	svc := service.NewService(repo)

	// Initialize router
	r := mux.NewRouter()
	r.Use(otelmux.Middleware("repair-service"))

	// Health check endpoint for Consul
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, span := otel.Tracer("repair-service").Start(r.Context(), "HealthCheck")
		defer span.End()
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	}).Methods("GET")

	// Create repair endpoint
	r.HandleFunc("/repairs", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("repair-service").Start(r.Context(), "CreateRepair")
		defer span.End()

		log.Println("Received POST /repairs request")
		var cost domain.RepairCostModel
		if err := json.NewDecoder(r.Body).Decode(&cost); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Invalid request body")
			log.Printf("Failed to decode request body: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Invalid request body: %v", err)})
			return
		}
		log.Printf("Decoded cost: %+v", cost)
		span.SetAttributes(
			attribute.String("userID", cost.UserID),
			attribute.String("repairType", cost.RepairType),
			attribute.Float64("totalPrice", cost.TotalPrice),
		)
		if cost.ID == "" {
			cost.ID = primitive.NewObjectID().Hex()
			log.Printf("Generated new ID for cost: %s", cost.ID)
			span.SetAttributes(attribute.String("costID", cost.ID))
		}
		repair, err := svc.CreateRepair(ctx, &cost)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to create repair")
			log.Printf("Failed to create repair: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Failed to create repair: %v", err)})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(repair); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to encode response")
			log.Printf("Failed to encode response: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Failed to encode response: %v", err)})
			return
		}
		log.Println("Successfully sent response for POST /repairs")
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
			log.Printf("Failed to decode request body: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid request body"})
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
			log.Printf("Failed to estimate repair cost: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Failed to estimate repair cost: %v", err)})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(cost); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to encode response")
			log.Printf("Failed to encode response: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Failed to encode response: %v", err)})
			return
		}
	}).Methods("POST")

	// Get all repairs endpoint
	r.HandleFunc("/repairs", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("repair-service").Start(r.Context(), "GetAllRepairs")
		defer span.End()

		log.Println("Received GET /repairs request")
		repairs, err := svc.GetAllRepairs(ctx)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to get repairs")
			log.Printf("Failed to get repairs: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Failed to get repairs: %v", err)})
			return
		}
		span.SetAttributes(
			attribute.Int("repairCount", len(repairs)),
		)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(repairs); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to encode response")
			log.Printf("Failed to encode response: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("Failed to encode response: %v", err)})
			return
		}
		log.Println("Successfully sent response for GET /repairs")
	}).Methods("GET")

// Start gRPC server in a separate goroutine
    go func() {
        grpcPort := os.Getenv("GRPC_PORT")
        if grpcPort == "" {
            grpcPort = "50051"
        }
        lis, err := net.Listen("tcp", ":"+grpcPort)
        if err != nil {
            log.Fatalf("Failed to listen for gRPC: %v", err)
        }
        grpcServer := grpc.NewServer()
        proto.RegisterRepairServiceServer(grpcServer, grpcsvc.NewRepairServer(repo))
        reflection.Register(grpcServer) // Enable reflection for debugging
        log.Printf("Starting gRPC server on port %s", grpcPort)
        if err := grpcServer.Serve(lis); err != nil {
            log.Fatalf("Failed to start gRPC server: %v", err)
        }
    }()

	// Start server
	port := os.Getenv("SERVICE_PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Starting repair-service on port %s", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
