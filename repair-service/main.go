package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"repair-service/domain"
	"repair-service/service"
	"time"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoRepository struct {
	repairCollection *mongo.Collection
	costCollection   *mongo.Collection
}

func NewMongoRepository(client *mongo.Client) *MongoRepository {
	return &MongoRepository{
		repairCollection: client.Database("repairdb").Collection("repairs"),
		costCollection:   client.Database("repairdb").Collection("repair_costs"),
	}
}

// Implement domain.RepairRepository
func (r *MongoRepository) CreateRepair(ctx context.Context, repair *domain.RepairModel) (*domain.RepairModel, error) {
	_, err := r.repairCollection.InsertOne(ctx, repair)
	return repair, err
}

func (r *MongoRepository) SaveRepairCost(ctx context.Context, cost *domain.RepairCostModel) error {
	_, err := r.costCollection.InsertOne(ctx, cost)
	return err
}

func (r *MongoRepository) GetRepairCostByID(ctx context.Context, id string) (*domain.RepairCostModel, error) {
	var cost domain.RepairCostModel
	err := r.costCollection.FindOne(ctx, map[string]string{"_id": id}).Decode(&cost)
	if err != nil {
		return nil, err
	}
	return &cost, nil
}

func (r *MongoRepository) GetRepairByID(ctx context.Context, id string) (*domain.RepairModel, error) {
	var repair domain.RepairModel
	err := r.repairCollection.FindOne(ctx, map[string]string{"_id": id}).Decode(&repair)
	if err != nil {
		return nil, err
	}
	return &repair, nil
}

func (r *MongoRepository) UpdateRepair(ctx context.Context, repairID string, status string) error {
	_, err := r.repairCollection.UpdateOne(ctx, map[string]string{"_id": repairID}, map[string]interface{}{"$set": map[string]string{"status": status}})
	return err
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
	// Connect to MongoDB with retries
	client, err := connectToMongoDB("mongodb://admin:admin@mongodb:27017", 5, 2*time.Second)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	// Initialize repository and service
	repo := NewMongoRepository(client)
	svc := service.NewService(repo)

	// Initialize router
	r := mux.NewRouter()

	// Define endpoints
	r.HandleFunc("/repairs", func(w http.ResponseWriter, r *http.Request) {
		log.Println("Received POST /repairs request")
		var cost domain.RepairCostModel
		if err := json.NewDecoder(r.Body).Decode(&cost); err != nil {
			log.Printf("Failed to decode request body: %v", err)
			http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
			return
		}
		log.Printf("Decoded cost: %+v", cost)
		// Ensure ID is set if not provided
		if cost.ID == "" {
			cost.ID = primitive.NewObjectID().Hex()
			log.Printf("Generated new ID for cost: %s", cost.ID)
		}
		repair, err := svc.CreateRepair(r.Context(), &cost)
		if err != nil {
			log.Printf("Failed to create repair: %v", err)
			http.Error(w, fmt.Sprintf("Failed to create repair: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(repair); err != nil {
			log.Printf("Failed to encode response: %v", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}
		log.Println("Successfully sent response for POST /repairs")
	}).Methods("POST")

	r.HandleFunc("/repairs/estimate", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			RepairType string `json:"repairType"`
			UserID     string `json:"userID"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		cost, err := svc.EstimateRepairCost(r.Context(), input.RepairType, input.UserID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cost)
	}).Methods("POST")

	r.HandleFunc("/repairs/cost/{costID}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		costID := vars["costID"]
		userID := r.URL.Query().Get("userID")
		cost, err := svc.GetAndValidateRepairCost(r.Context(), costID, userID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cost)
	}).Methods("GET")

	r.HandleFunc("/repairs/{repairID}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		repairID := vars["repairID"]
		repair, err := svc.GetRepairByID(r.Context(), repairID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(repair)
	}).Methods("GET")

	r.HandleFunc("/repairs/{repairID}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		repairID := vars["repairID"]
		var input struct {
			Status string `json:"status"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		if err := svc.UpdateRepair(r.Context(), repairID, input.Status); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
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
