package handlers

import (
	"bytes"
	"encoding/json"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
)

// RepairCostModel mirrors repair-service's domain.RepairCostModel
type RepairCostModel struct {
	ID          string  `json:"id"`
	UserID      string  `json:"userID"`
	RepairType  string  `json:"repairType"`
	TotalPrice  float64 `json:"totalPrice"`
}

// RepairModel mirrors repair-service's domain.RepairModel
type RepairModel struct {
	ID         string           `json:"id"`
	UserID     string           `json:"userID"`
	Status     string           `json:"status"`
	RepairCost *RepairCostModel `json:"repairCost"`
}

// RepairHandler handles HTTP requests for repair operations
type RepairHandler struct {
	repairServiceURL string
	client           *http.Client
}

// NewRepairHandler creates a new RepairHandler
func NewRepairHandler(repairServiceURL string) *RepairHandler {
	return &RepairHandler{
		repairServiceURL: repairServiceURL,
		client:           &http.Client{},
	}
}

// CreateRepair forwards a repair creation request to repair-service
func (h *RepairHandler) CreateRepair(w http.ResponseWriter, r *http.Request) {
	var cost RepairCostModel
	if err := json.NewDecoder(r.Body).Decode(&cost); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	body, err := json.Marshal(cost)
	if err != nil {
		http.Error(w, "Failed to marshal request", http.StatusInternalServerError)
		return
	}

	resp, err := h.client.Post(h.repairServiceURL+"/repairs", "application/json", bytes.NewBuffer(body))
	if err != nil {
		http.Error(w, "Failed to contact repair service", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	// Log the raw response for debugging
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read response body: %v", err)
		http.Error(w, "Failed to read response", http.StatusInternalServerError)
		return
	}
	log.Printf("Repair service response: %s", string(bodyBytes))
	resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // Restore body for decoding

	var repair RepairModel
	if err := json.NewDecoder(resp.Body).Decode(&repair); err != nil {
		log.Printf("Failed to decode response: %v", err)
		http.Error(w, "Failed to decode response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	json.NewEncoder(w).Encode(repair)
}

// EstimateRepairCost forwards a cost estimation request to repair-service
func (h *RepairHandler) EstimateRepairCost(w http.ResponseWriter, r *http.Request) {
	var input struct {
		RepairType string `json:"repairType"`
		UserID     string `json:"userID"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	body, err := json.Marshal(input)
	if err != nil {
		http.Error(w, "Failed to marshal request", http.StatusInternalServerError)
		return
	}

	resp, err := h.client.Post(h.repairServiceURL+"/repairs/estimate", "application/json", bytes.NewBuffer(body))
	if err != nil {
		http.Error(w, "Failed to contact repair service", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	var cost RepairCostModel
	if err := json.NewDecoder(resp.Body).Decode(&cost); err != nil {
		http.Error(w, "Failed to decode response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	json.NewEncoder(w).Encode(cost)
}

// GetRepairCost retrieves a repair cost by ID
func (h *RepairHandler) GetRepairCost(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	costID := vars["costID"]
	userID := r.URL.Query().Get("userID")

	resp, err := h.client.Get(h.repairServiceURL + "/repairs/cost/" + costID + "?userID=" + userID)
	if err != nil {
		http.Error(w, "Failed to contact repair service", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	var cost RepairCostModel
	if err := json.NewDecoder(resp.Body).Decode(&cost); err != nil {
		http.Error(w, "Failed to decode response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	json.NewEncoder(w).Encode(cost)
}

// GetRepair retrieves a repair by ID
func (h *RepairHandler) GetRepair(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repairID := vars["repairID"]

	resp, err := h.client.Get(h.repairServiceURL + "/repairs/" + repairID)
	if err != nil {
		http.Error(w, "Failed to contact repair service", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	var repair RepairModel
	if err := json.NewDecoder(resp.Body).Decode(&repair); err != nil {
		http.Error(w, "Failed to decode response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	json.NewEncoder(w).Encode(repair)
}

// UpdateRepair updates a repair's status
func (h *RepairHandler) UpdateRepair(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repairID := vars["repairID"]

	var input struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	body, err := json.Marshal(input)
	if err != nil {
		http.Error(w, "Failed to marshal request", http.StatusInternalServerError)
		return
	}

	req, err := http.NewRequest("PUT", h.repairServiceURL+"/repairs/"+repairID, bytes.NewBuffer(body))
	if err != nil {
		http.Error(w, "Failed to create request", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		http.Error(w, "Failed to contact repair service", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
}
