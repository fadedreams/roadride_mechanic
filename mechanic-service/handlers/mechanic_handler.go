package handlers

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"log"
	"math"
	"net/http"
)

// Repair represents a repair request
type Repair struct {
	ID         string       `json:"id" bson:"_id"`
	UserID     string       `json:"userID" bson:"userID"`
	Status     string       `json:"status" bson:"status"`
	RepairCost *RepairCost  `json:"repairCost" bson:"repairCost"`
	AssignedTo string       `json:"assignedTo" bson:"assignedTo,omitempty"`
}

// RepairCost represents the cost details of a repair
type RepairCost struct {
	ID           string         `json:"id" bson:"_id"`
	UserID       string         `json:"userID" bson:"userID"`
	RepairType   string         `json:"repairType" bson:"repairType"`
	TotalPrice   float64        `json:"totalPrice" bson:"totalPrice"`
	UserLocation *Location      `json:"userLocation" bson:"userLocation,omitempty"`
	Mechanics    []MechanicInfo `json:"mechanics" bson:"mechanics,omitempty"`
}

// Location represents geographic coordinates
type Location struct {
	Latitude  float64 `json:"latitude" bson:"latitude"`
	Longitude float64 `json:"longitude" bson:"longitude"`
}

// Mechanic represents a mechanic
type Mechanic struct {
	ID       string   `json:"id" bson:"_id"`
	Name     string   `json:"name" bson:"name"`
	Location Location `json:"location" bson:"location"`
}

// MechanicInfo represents a mechanic with distance from user
type MechanicInfo struct {
	ID       string   `json:"id" bson:"id"`
	Name     string   `json:"name" bson:"name"`
	Location Location `json:"location" bson:"location"`
	Distance float64  `json:"distance" bson:"distance"`
}

// MechanicHandler handles mechanic service requests
type MechanicHandler struct {
	client *mongo.Client
	tracer trace.Tracer
}

// NewMechanicHandler creates a new MechanicHandler
func NewMechanicHandler(client *mongo.Client) *MechanicHandler {
	tracer := otel.Tracer("mechanic-service")
	return &MechanicHandler{
		client: client,
		tracer: tracer,
	}
}

// haversine calculates the distance between two points in kilometers
func haversine(l1, l2 Location) float64 {
	const R = 6371 // Earth's radius in km
	lat1 := l1.Latitude * math.Pi / 180
	lat2 := l2.Latitude * math.Pi / 180
	dLat := (l2.Latitude - l1.Latitude) * math.Pi / 180
	dLon := (l2.Longitude - l1.Longitude) * math.Pi / 180

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}

// HealthCheck provides a health endpoint
func (h *MechanicHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	_, span := h.tracer.Start(r.Context(), "HealthCheck")
	defer span.End()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// ListNearbyRepairs lists repairs within 10km of a specified mechanic's location
func (h *MechanicHandler) ListNearbyRepairs(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.Start(r.Context(), "ListNearbyRepairs")
	defer span.End()

	mechanicID := r.URL.Query().Get("mechanicID")
	if mechanicID == "" {
		span.SetStatus(codes.Error, "Mechanic ID is required")
		http.Error(w, "Mechanic ID is required", http.StatusBadRequest)
		return
	}

	// Query mechanic from MongoDB
	mechanicCollection := h.client.Database("repairdb").Collection("mechanics")
	var mechanic Mechanic
	err := mechanicCollection.FindOne(ctx, bson.M{"_id": mechanicID}).Decode(&mechanic)
	if err != nil {
		log.Printf("Failed to find mechanic %s: %v", mechanicID, err)
		span.SetStatus(codes.Error, "Mechanic not found")
		http.Error(w, "Mechanic not found", http.StatusNotFound)
		return
	}
	mechanicLoc := mechanic.Location
	log.Printf("Mechanic %s location: %+v", mechanicID, mechanicLoc)
	span.SetAttributes(attribute.String("mechanicID", mechanicID))

	// Query repairs
	repairCollection := h.client.Database("repairdb").Collection("repairs")
	cursor, err := repairCollection.Find(ctx, bson.M{})
	if err != nil {
		log.Printf("Failed to query repairs: %v", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to query repairs")
		http.Error(w, "Failed to query repairs", http.StatusInternalServerError)
		return
	}
	defer cursor.Close(ctx)

	var repairs []Repair
	if err := cursor.All(ctx, &repairs); err != nil {
		log.Printf("Failed to decode repairs: %v", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to decode repairs")
		http.Error(w, "Failed to decode repairs", http.StatusInternalServerError)
		return
	}
	log.Printf("Found %d repairs", len(repairs))
	for i, repair := range repairs {
		log.Printf("Repair %d: ID=%s, RepairCost=%+v", i, repair.ID, repair.RepairCost)
	}

	var nearby []Repair
	for _, repair := range repairs {
		if repair.RepairCost != nil && repair.RepairCost.UserLocation != nil {
			distance := haversine(mechanicLoc, *repair.RepairCost.UserLocation)
			log.Printf("Repair %s: Distance to mechanic %s = %.2f km", repair.ID, mechanicID, distance)
			if distance <= 10 {
				nearby = append(nearby, repair)
				log.Printf("Repair %s added to nearby", repair.ID)
			}
		} else {
			log.Printf("Repair %s skipped: RepairCost or UserLocation is nil", repair.ID)
		}
	}
	span.SetAttributes(attribute.Int("nearby_repair_count", len(nearby)))

	w.Header().Set("Content-Type", "application/json")
	if len(nearby) == 0 {
		log.Printf("No nearby repairs found for mechanic %s", mechanicID)
		w.Write([]byte("[]"))
	} else {
		log.Printf("Returning %d nearby repairs", len(nearby))
		json.NewEncoder(w).Encode(nearby)
	}
}

// AssignRepair assigns a mechanic to a repair
func (h *MechanicHandler) AssignRepair(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.Start(r.Context(), "AssignRepair")
	defer span.End()

	vars := mux.Vars(r)
	repairID := vars["repairID"]

	var input struct {
		MechanicID string `json:"mechanicID"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Invalid request body")
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Connect to MongoDB collection
	collection := h.client.Database("repairdb").Collection("repairs")
	var repair Repair
	if err := collection.FindOne(ctx, bson.M{"_id": repairID}).Decode(&repair); err != nil {
		span.SetStatus(codes.Error, "Repair not found")
		http.Error(w, "Repair not found", http.StatusNotFound)
		return
	}

	// Validate mechanic by querying MongoDB
	mechanicCollection := h.client.Database("repairdb").Collection("mechanics")
	var mechanic Mechanic
	if err := mechanicCollection.FindOne(ctx, bson.M{"_id": input.MechanicID}).Decode(&mechanic); err != nil {
		span.SetStatus(codes.Error, "Mechanic not found")
		http.Error(w, "Mechanic not found", http.StatusBadRequest)
		return
	}

	// Update repair with assignment
	update := bson.M{"$set": bson.M{"assignedTo": input.MechanicID}}
	if _, err := collection.UpdateOne(ctx, bson.M{"_id": repairID}, update); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to assign repair")
		http.Error(w, "Failed to assign repair", http.StatusInternalServerError)
		return
	}

	repair.AssignedTo = input.MechanicID
	span.SetAttributes(attribute.String("repairID", repairID), attribute.String("mechanicID", input.MechanicID))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(repair)
}
