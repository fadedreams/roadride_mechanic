// mechanic-service/handlers/mechanic_handler.go
package handlers

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"mechanic-service/service"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// MechanicHandler handles mechanic service requests
type MechanicHandler struct {
	service *service.Service
	tracer  trace.Tracer
}

// NewMechanicHandler creates a new MechanicHandler
func NewMechanicHandler(service *service.Service) *MechanicHandler {
	return &MechanicHandler{
		service: service,
		tracer:  otel.Tracer("mechanic-service"),
	}
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

	nearby, err := h.service.ListNearbyRepairs(ctx, mechanicID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	span.SetAttributes(
		attribute.String("mechanicID", mechanicID),
		attribute.Int("nearbyRepairCount", len(nearby)),
	)

	w.Header().Set("Content-Type", "application/json")
	if len(nearby) == 0 {
		w.Write([]byte("[]"))
	} else {
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

	repair, err := h.service.AssignRepair(ctx, repairID, input.MechanicID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	span.SetAttributes(
		attribute.String("repairID", repairID),
		attribute.String("mechanicID", input.MechanicID),
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(repair)
}
