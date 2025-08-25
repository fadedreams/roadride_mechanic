package handlers

import (
	"bytes"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"io"
	"log"
	"net/http"
	"sync"
)

// RepairCostModel mirrors repair-service's domain.RepairCostModel
type RepairCostModel struct {
	ID           string         `json:"id"`
	UserID       string         `json:"userID"`
	RepairType   string         `json:"repairType"`
	TotalPrice   float64        `json:"totalPrice"`
	UserLocation *Location       `json:"userLocation,omitempty"`
	Mechanics    []MechanicInfo `json:"mechanics,omitempty"`
}

// Location mirrors repair-service's domain.Location
type Location struct {
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
}

// MechanicInfo mirrors repair-service's domain.MechanicInfo
type MechanicInfo struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Location Location `json:"location"`
	Distance float64 `json:"distance"`
}

// RepairModel mirrors repair-service's domain.RepairModel
type RepairModel struct {
	ID         string           `json:"id"`
	UserID     string           `json:"userID"`
	Status     string           `json:"status"`
	RepairCost *RepairCostModel `json:"repairCost"`
}

// WebSocket message for status updates
type StatusUpdate struct {
	RepairID string `json:"repairID"`
	UserID   string `json:"userID"`
	Status   string `json:"status"`
}

// RepairHandler handles HTTP and WebSocket requests for repair operations
type RepairHandler struct {
	repairServiceURL string
	client           *http.Client
	upgrader         websocket.Upgrader
	clients          map[string][]*websocket.Conn // Map of userID to WebSocket connections
	clientsMutex     sync.Mutex
}

// NewRepairHandler creates a new RepairHandler
func NewRepairHandler(repairServiceURL string) *RepairHandler {
	return &RepairHandler{
		repairServiceURL: repairServiceURL,
		client:           &http.Client{},
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for simplicity; adjust for production
			},
		},
		clients: make(map[string][]*websocket.Conn),
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
	resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

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
		RepairType string   `json:"repairType"`
		UserID     string   `json:"userID"`
		Location   Location `json:"location"`
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

	// Log the raw response for debugging
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read response body: %v", err)
		http.Error(w, "Failed to read response", http.StatusInternalServerError)
		return
	}
	log.Printf("Repair service response: %s", string(bodyBytes))
	resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	var cost RepairCostModel
	if err := json.NewDecoder(resp.Body).Decode(&cost); err != nil {
		log.Printf("Failed to decode response: %v", err)
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

// UpdateRepair updates a repair's status and broadcasts to WebSocket clients
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

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Printf("Repair service error: %s", string(bodyBytes))
		http.Error(w, "Failed to update repair", resp.StatusCode)
		return
	}

	// Get the repair to obtain userID for broadcasting
	repairResp, err := h.client.Get(h.repairServiceURL + "/repairs/" + repairID)
	if err != nil {
		log.Printf("Failed to fetch repair for broadcasting: %v", err)
	} else {
		var repair RepairModel
		if err := json.NewDecoder(repairResp.Body).Decode(&repair); err == nil {
			// Broadcast status update to clients
			update := StatusUpdate{
				RepairID: repairID,
				UserID:   repair.UserID,
				Status:   input.Status,
			}
			h.broadcastStatusUpdate(update)
		} else {
			log.Printf("Failed to decode repair for broadcasting: %v", err)
		}
		repairResp.Body.Close()
	}

	w.WriteHeader(resp.StatusCode)
}

// HandleWebSocket manages WebSocket connections
func (h *RepairHandler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userID")
	if userID == "" {
		http.Error(w, "userID is required", http.StatusBadRequest)
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade to WebSocket: %v", err)
		http.Error(w, "Failed to upgrade to WebSocket", http.StatusInternalServerError)
		return
	}

	// Register client
	h.clientsMutex.Lock()
	h.clients[userID] = append(h.clients[userID], conn)
	h.clientsMutex.Unlock()
	log.Printf("WebSocket client connected for userID: %s", userID)

	// Handle client disconnection
	defer func() {
		h.clientsMutex.Lock()
		clients := h.clients[userID]
		for i, c := range clients {
			if c == conn {
				h.clients[userID] = append(clients[:i], clients[i+1:]...)
				break
			}
		}
		if len(h.clients[userID]) == 0 {
			delete(h.clients, userID)
		}
		h.clientsMutex.Unlock()
		conn.Close()
		log.Printf("WebSocket client disconnected for userID: %s", userID)
	}()

	// Keep connection alive (handle incoming messages if needed)
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			log.Printf("WebSocket read error: %v", err)
			break
		}
	}
}

// broadcastStatusUpdate sends status updates to all clients subscribed to the userID
func (h *RepairHandler) broadcastStatusUpdate(update StatusUpdate) {
	h.clientsMutex.Lock()
	defer h.clientsMutex.Unlock()

	clients, exists := h.clients[update.UserID]
	if !exists {
		return
	}

	message, err := json.Marshal(update)
	if err != nil {
		log.Printf("Failed to marshal status update: %v", err)
		return
	}

	for _, conn := range clients {
		err := conn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			log.Printf("Failed to send WebSocket message: %v", err)
			conn.Close()
		}
	}
}
