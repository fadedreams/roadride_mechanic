package handlers

import (
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"log"
	"math"
	"net/http"
	"sync"
	"time"
)

// Repair represents a repair request
type Repair struct {
	ID         string    `json:"id"`
	UserID     string    `json:"userID"`
	RepairType string    `json:"repairType"`
	Location   Location  `json:"location"`
	AssignedTo string    `json:"assignedTo,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}

// Location represents geographic coordinates
type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// Mechanic represents a mechanic
type Mechanic struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Location Location `json:"location"`
}

// MechanicHandler handles mechanic service requests
type MechanicHandler struct {
	repairs   map[string]Repair
	mechanics []Mechanic
	mu        sync.RWMutex
	tracer    trace.Tracer
}

// NewMechanicHandler creates a new MechanicHandler
func NewMechanicHandler() *MechanicHandler {
	tracer := otel.Tracer("mechanic-service")
	h := &MechanicHandler{
		repairs: make(map[string]Repair),
		mechanics: []Mechanic{
			{ID: "m1", Name: "Mechanic1", Location: Location{Latitude: 52.5200, Longitude: 13.4050}},
			{ID: "m2", Name: "Mechanic2", Location: Location{Latitude: 52.5100, Longitude: 13.4150}},
		},
		tracer: tracer,
	}
	return h
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
	ctx, span := h.tracer.Start(r.Context(), "HealthCheck")
	defer span.End()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// ListNearbyRepairs lists repairs within 10km of a mechanic's location
func (h *MechanicHandler) ListNearbyRepairs(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.Start(r.Context(), "ListNearbyRepairs")
	defer span.End()

	// Simulate mechanic location (e.g., first mechanic)
	mechanicLoc := h.mechanics[0].Location

	h.mu.RLock()
	defer h.mu.RUnlock()

	var nearby []Repair
	for _, repair := range h.repairs {
		distance := haversine(mechanicLoc, repair.Location)
		if distance <= 10 {
			nearby = append(nearby, repair)
		}
	}
	span.SetAttributes(attribute.Int("nearby_repair_count", len(nearby)))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nearby)
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

	h.mu.Lock()
	defer h.mu.Unlock()

	repair, exists := h.repairs[repairID]
	if !exists {
		span.SetStatus(codes.Error, "Repair not found")
		http.Error(w, "Repair not found", http.StatusNotFound)
		return
	}

	// Validate mechanic
	var mechanic *Mechanic
	for i := range h.mechanics {
		if h.mechanics[i].ID == input.MechanicID {
			mechanic = &h.mechanics[i]
			break
		}
	}
	if mechanic == nil {
		span.SetStatus(codes.Error, "Mechanic not found")
		http.Error(w, "Mechanic not found", http.StatusBadRequest)
		return
	}

	repair.AssignedTo = input.MechanicID
	h.repairs[repairID] = repair
	span.SetAttributes(attribute.String("repairID", repairID), attribute.String("mechanicID", input.MechanicID))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(repair)
}
