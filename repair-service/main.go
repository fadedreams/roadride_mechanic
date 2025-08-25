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

	"github.com/gorilla/mux"
	"github.com/hashicorp/consul/api"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
)

type MongoRepository struct {
	repairCollection  *mongo.Collection
	costCollection    *mongo.Collection
	mechanicCollection *mongo.Collection
}

func NewMongoRepository(client *mongo.Client) *MongoRepository {
	return &MongoRepository{
		repairCollection:  client.Database("repairdb").Collection("repairs"),
		costCollection:    client.Database("repairdb").Collection("repair_costs"),
		mechanicCollection: client.Database("repairdb").Collection("mechanics"),
	}
}

// initTracer initializes OpenTelemetry tracer
func initTracer() (func(), error) {
	jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "http://jaeger:4318/v1/traces"
	}
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint("jaeger:4318"),
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithURLPath("/v1/traces"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %v", err)
	}

	resources := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("repair-service"),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter)),
		sdktrace.WithResource(resources),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}, nil
}

// CreateRepair inserts a new repair
func (r *MongoRepository) CreateRepair(ctx context.Context, repair *domain.RepairModel) (*domain.RepairModel, error) {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoCreateRepair")
	defer span.End()

	_, err := r.repairCollection.InsertOne(ctx, repair)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to insert repair")
		return nil, err
	}
	span.SetAttributes(
		attribute.String("repairID", repair.ID),
		attribute.String("userID", repair.UserID),
		attribute.String("status", repair.Status),
	)
	return repair, nil
}

// SaveRepairCost inserts a new repair cost
func (r *MongoRepository) SaveRepairCost(ctx context.Context, cost *domain.RepairCostModel) error {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoSaveRepairCost")
	defer span.End()

	_, err := r.costCollection.InsertOne(ctx, cost)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to insert repair cost")
		return err
	}
	span.SetAttributes(
		attribute.String("costID", cost.ID),
		attribute.String("userID", cost.UserID),
		attribute.String("repairType", cost.RepairType),
		attribute.Float64("totalPrice", cost.TotalPrice),
	)
	return nil
}

// GetRepairCostByID retrieves a repair cost by ID
func (r *MongoRepository) GetRepairCostByID(ctx context.Context, id string) (*domain.RepairCostModel, error) {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoGetRepairCostByID")
	defer span.End()

	var cost domain.RepairCostModel
	err := r.costCollection.FindOne(ctx, bson.M{"_id": id}).Decode(&cost)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find repair cost")
		return nil, err
	}
	span.SetAttributes(
		attribute.String("costID", id),
		attribute.String("userID", cost.UserID),
	)
	return &cost, nil
}

// GetRepairByID retrieves a repair by ID
func (r *MongoRepository) GetRepairByID(ctx context.Context, id string) (*domain.RepairModel, error) {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoGetRepairByID")
	defer span.End()

	var repair domain.RepairModel
	err := r.repairCollection.FindOne(ctx, bson.M{"_id": id}).Decode(&repair)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find repair")
		return nil, err
	}
	span.SetAttributes(
		attribute.String("repairID", id),
		attribute.String("userID", repair.UserID),
	)
	return &repair, nil
}

// UpdateRepair updates the status of a repair
func (r *MongoRepository) UpdateRepair(ctx context.Context, repairID string, status string) error {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoUpdateRepair")
	defer span.End()

	_, err := r.repairCollection.UpdateOne(ctx, bson.M{"_id": repairID}, bson.M{"$set": bson.M{"status": status}})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to update repair")
		return err
	}
	span.SetAttributes(
		attribute.String("repairID", repairID),
		attribute.String("status", status),
	)
	return nil
}

// GetAllMechanics retrieves all mechanics
func (r *MongoRepository) GetAllMechanics(ctx context.Context) ([]*domain.MechanicModel, error) {
	_, span := otel.Tracer("repair-service").Start(ctx, "MongoGetAllMechanics")
	defer span.End()

	var mechanics []*domain.MechanicModel
	cursor, err := r.mechanicCollection.Find(ctx, bson.M{})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to find mechanics")
		return nil, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var mechanic domain.MechanicModel
		if err := cursor.Decode(&mechanic); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to decode mechanic")
			return nil, err
		}
		mechanics = append(mechanics, &mechanic)
	}
	if err := cursor.Err(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Cursor error")
		return nil, err
	}
	span.SetAttributes(
		attribute.Int("mechanicCount", len(mechanics)),
	)
	return mechanics, nil
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
				cancel()
				return client, nil
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
	repo := NewMongoRepository(client)
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

	// Define endpoints
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

	r.HandleFunc("/repairs/estimate", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("repair-service").Start(r.Context(), "EstimateRepairCost")
		defer span.End()

		var input struct {
			RepairType string         `json:"repairType"`
			UserID     string         `json:"userID"`
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
			attribute.String("userID", input.UserID),
			attribute.String("repairType", input.RepairType),
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
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
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

	r.HandleFunc("/repairs/cost/{costID}", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("repair-service").Start(r.Context(), "GetRepairCost")
		defer span.End()

		vars := mux.Vars(r)
		costID := vars["costID"]
		userID := r.URL.Query().Get("userID")
		span.SetAttributes(
			attribute.String("costID", costID),
			attribute.String("userID", userID),
		)
		cost, err := svc.GetAndValidateRepairCost(ctx, costID, userID)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to get repair cost")
			log.Printf("Failed to get repair cost: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
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
	}).Methods("GET")

	r.HandleFunc("/repairs/{repairID}", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("repair-service").Start(r.Context(), "GetRepair")
		defer span.End()

		vars := mux.Vars(r)
		repairID := vars["repairID"]
		span.SetAttributes(
			attribute.String("repairID", repairID),
		)
		repair, err := svc.GetRepairByID(ctx, repairID)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to get repair")
			log.Printf("Failed to get repair: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
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
	}).Methods("GET")

	r.HandleFunc("/repairs/{repairID}", func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("repair-service").Start(r.Context(), "UpdateRepair")
		defer span.End()

		vars := mux.Vars(r)
		repairID := vars["repairID"]
		var input struct {
			Status string `json:"status"`
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
			attribute.String("repairID", repairID),
			attribute.String("status", input.Status),
		)
		if err := svc.UpdateRepair(ctx, repairID, input.Status); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to update repair")
			log.Printf("Failed to update repair: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusOK)
	}).Methods("PUT")

	// Start server
	log.Println("Repair Service running on port 8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
