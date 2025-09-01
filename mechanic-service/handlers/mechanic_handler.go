package handlers

import (
	"encoding/json"
	"log/slog"
	"mechanic-service/service"
	"net/http"

	"github.com/gorilla/mux"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// MechanicHandler handles mechanic service requests
type MechanicHandler struct {
	service *service.Service
	tracer  trace.Tracer
	logger  *slog.Logger
}

// NewMechanicHandler creates a new MechanicHandler
func NewMechanicHandler(service *service.Service, logger *slog.Logger) *MechanicHandler {
	return &MechanicHandler{
		service: service,
		tracer:  otel.Tracer("mechanic-service"),
		logger:  logger,
	}
}

// HealthCheck provides a health endpoint
func (h *MechanicHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	_, span := h.tracer.Start(r.Context(), "HealthCheck")
	defer span.End()

	h.logger.Info("Health check requested", "app", "mechanic-service")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// ListNearbyRepairs lists repairs within 10km of a specified mechanic's location
func (h *MechanicHandler) ListNearbyRepairs(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.Start(r.Context(), "ListNearbyRepairs")
	defer span.End()

	h.logger.Info("Received GET /repairs/nearby request", "app", "mechanic-service")
	mechanicID := r.URL.Query().Get("mechanicID")
	if mechanicID == "" {
		span.SetStatus(codes.Error, "Mechanic ID is required")
		h.logger.Error("Mechanic ID is required", "app", "mechanic-service")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Mechanic ID is required"})
		return
	}

	nearby, err := h.service.ListNearbyRepairs(ctx, mechanicID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		h.logger.Error("Failed to list nearby repairs", "error", err, "mechanicID", mechanicID, "app", "mechanic-service")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	span.SetAttributes(
		attribute.String("mechanicID", mechanicID),
		attribute.Int("nearbyRepairCount", len(nearby)),
	)

	h.logger.Info("Successfully sent response for GET /repairs/nearby", "repairCount", len(nearby), "app", "mechanic-service")
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

	h.logger.Info("Received POST /repairs/{repairID}/assign request", "app", "mechanic-service")
	vars := mux.Vars(r)
	repairID := vars["repairID"]

	var input struct {
		MechanicID string `json:"mechanicID"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Invalid request body")
		h.logger.Error("Failed to decode request body", "error", err, "repairID", repairID, "app", "mechanic-service")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Invalid request body: " + err.Error()})
		return
	}

	repair, err := h.service.AssignRepair(ctx, repairID, input.MechanicID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		h.logger.Error("Failed to assign repair", "error", err, "repairID", repairID, "mechanicID", input.MechanicID, "app", "mechanic-service")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	span.SetAttributes(
		attribute.String("repairID", repairID),
		attribute.String("mechanicID", input.MechanicID),
	)

	h.logger.Info("Successfully assigned repair", "repairID", repairID, "mechanicID", input.MechanicID, "app", "mechanic-service")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(repair)
}
