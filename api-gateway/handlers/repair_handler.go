package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/consul/api"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
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
	client           *http.Client
	consulClient     *api.Client
	repairServiceURL string
	upgrader         websocket.Upgrader
	clients          map[string][]*websocket.Conn // Map of userID to WebSocket connections
	clientsMutex     sync.Mutex
	tracer           trace.Tracer
}

// initTracer initializes OpenTelemetry tracer
func initTracer() (func(), error) {
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
		semconv.ServiceNameKey.String("api-gateway"),
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

// NewRepairHandler creates a new RepairHandler with Consul and Jaeger integration
func NewRepairHandler() *RepairHandler {
	// Initialize tracer
	shutdown, err := initTracer()
	if err != nil {
		log.Fatalf("Failed to initialize tracer: %v", err)
	}
	defer shutdown()

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
		serviceName = "api-gateway"
	}
	servicePort := os.Getenv("SERVICE_PORT")
	if servicePort == "" {
		servicePort = "8081"
	}
	serviceID := serviceName + "-" + servicePort
	registration := &api.AgentServiceRegistration{
		ID:      serviceID,
		Name:    serviceName,
		Port:    8081,
		Address: "api-gateway",
		Check: &api.AgentServiceCheck{
			HTTP:     fmt.Sprintf("http://api-gateway:8081/health"),
			Interval: "10s",
			Timeout:  "5s",
		},
	}
	if err := consulClient.Agent().ServiceRegister(registration); err != nil {
		log.Fatalf("Failed to register with Consul: %v", err)
	}

	// Discover repair-service
	repairServiceURL := ""
	for {
		services, _, err := consulClient.Health().Service("repair-service", "", true, nil)
		if err != nil {
			log.Printf("Failed to discover repair-service: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if len(services) > 0 {
			repairServiceURL = fmt.Sprintf("http://%s:%d", services[0].Service.Address, services[0].Service.Port)
			log.Printf("Discovered repair-service at: %s", repairServiceURL)
			break
		}
		log.Println("Waiting for repair-service to be registered...")
		time.Sleep(2 * time.Second)
	}

	tracer := otel.Tracer("api-gateway")

	// Create HTTP client with OpenTelemetry instrumentation
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{},
	}

	return &RepairHandler{
		client:           client,
		consulClient:     consulClient,
		repairServiceURL: repairServiceURL,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for simplicity
			},
		},
		clients: make(map[string][]*websocket.Conn),
		tracer:  tracer,
	}
}

// HealthCheck provides a health endpoint for Consul
func (h *RepairHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	_, span := h.tracer.Start(r.Context(), "HealthCheck")
	defer span.End()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "OK")
}

// CreateRepair forwards a repair creation request to repair-service
func (h *RepairHandler) CreateRepair(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.Start(r.Context(), "CreateRepair")
	defer span.End()

	var cost RepairCostModel
	if err := json.NewDecoder(r.Body).Decode(&cost); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Invalid request body")
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	span.SetAttributes(
		attribute.String("userID", cost.UserID),
		attribute.String("repairType", cost.RepairType),
	)

	body, err := json.Marshal(cost)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to marshal request")
		http.Error(w, "Failed to marshal request", http.StatusInternalServerError)
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", h.repairServiceURL+"/repairs", bytes.NewBuffer(body))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to create request")
		http.Error(w, "Failed to create request", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := h.client.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to contact repair service")
		http.Error(w, "Failed to contact repair service", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to read response body")
		log.Printf("Failed to read response body: %v", err)
		http.Error(w, "Failed to read response", http.StatusInternalServerError)
		return
	}
	log.Printf("Repair service response: %s", string(bodyBytes))
	resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	var repair RepairModel
	if err := json.NewDecoder(resp.Body).Decode(&repair); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to decode response")
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
	ctx, span := h.tracer.Start(r.Context(), "EstimateRepairCost")
	defer span.End()

	var input struct {
		RepairType string   `json:"repairType"`
		UserID     string   `json:"userID"`
		Location   Location `json:"location"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Invalid request body")
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	span.SetAttributes(
		attribute.String("userID", input.UserID),
		attribute.String("repairType", input.RepairType),
	)

	body, err := json.Marshal(input)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to marshal request")
		http.Error(w, "Failed to marshal request", http.StatusInternalServerError)
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", h.repairServiceURL+"/repairs/estimate", bytes.NewBuffer(body))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to create request")
		http.Error(w, "Failed to create request", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := h.client.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to contact repair service")
		http.Error(w, "Failed to contact repair service", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to read response body")
		log.Printf("Failed to read response body: %v", err)
		http.Error(w, "Failed to read response", http.StatusInternalServerError)
		return
	}
	log.Printf("Repair service response: %s", string(bodyBytes))
	resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	var cost RepairCostModel
	if err := json.NewDecoder(resp.Body).Decode(&cost); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to decode response")
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
	ctx, span := h.tracer.Start(r.Context(), "GetRepairCost")
	defer span.End()

	vars := mux.Vars(r)
	costID := vars["costID"]
	userID := r.URL.Query().Get("userID")
	span.SetAttributes(
		attribute.String("costID", costID),
		attribute.String("userID", userID),
	)

	req, err := http.NewRequestWithContext(ctx, "GET", h.repairServiceURL+"/repairs/cost/"+costID+"?userID="+userID, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to create request")
		http.Error(w, "Failed to create request", http.StatusInternalServerError)
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := h.client.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to contact repair service")
		http.Error(w, "Failed to contact repair service", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	var cost RepairCostModel
	if err := json.NewDecoder(resp.Body).Decode(&cost); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to decode response")
		http.Error(w, "Failed to decode response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	json.NewEncoder(w).Encode(cost)
}

// GetRepair retrieves a repair by ID
func (h *RepairHandler) GetRepair(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.Start(r.Context(), "GetRepair")
	defer span.End()

	vars := mux.Vars(r)
	repairID := vars["repairID"]
	span.SetAttributes(attribute.String("repairID", repairID))

	req, err := http.NewRequestWithContext(ctx, "GET", h.repairServiceURL+"/repairs/"+repairID, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to create request")
		http.Error(w, "Failed to create request", http.StatusInternalServerError)
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := h.client.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to contact repair service")
		http.Error(w, "Failed to contact repair service", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	var repair RepairModel
	if err := json.NewDecoder(resp.Body).Decode(&repair); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to decode response")
		http.Error(w, "Failed to decode response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	json.NewEncoder(w).Encode(repair)
}

// UpdateRepair updates a repair's status and broadcasts to WebSocket clients
func (h *RepairHandler) UpdateRepair(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.Start(r.Context(), "UpdateRepair")
	defer span.End()

	vars := mux.Vars(r)
	repairID := vars["repairID"]
	span.SetAttributes(attribute.String("repairID", repairID))

	var input struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Invalid request body")
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	span.SetAttributes(attribute.String("status", input.Status))

	body, err := json.Marshal(input)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to marshal request")
		http.Error(w, "Failed to marshal request", http.StatusInternalServerError)
		return
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", h.repairServiceURL+"/repairs/"+repairID, bytes.NewBuffer(body))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to create request")
		http.Error(w, "Failed to create request", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := h.client.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to contact repair service")
		http.Error(w, "Failed to contact repair service", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		span.RecordError(fmt.Errorf("repair service error: %s", string(bodyBytes)))
		span.SetStatus(codes.Error, "Failed to update repair")
		log.Printf("Repair service error: %s", string(bodyBytes))
		http.Error(w, "Failed to update repair", resp.StatusCode)
		return
	}

	// Get the repair to obtain userID for broadcasting
	repairReq, err := http.NewRequestWithContext(ctx, "GET", h.repairServiceURL+"/repairs/"+repairID, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to create request for broadcasting")
		log.Printf("Failed to create request for broadcasting: %v", err)
		http.Error(w, "Failed to create request for broadcasting", http.StatusInternalServerError)
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(repairReq.Header))
	repairResp, err := h.client.Do(repairReq)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to fetch repair for broadcasting")
		log.Printf("Failed to fetch repair for broadcasting: %v", err)
		http.Error(w, "Failed to fetch repair for broadcasting", http.StatusInternalServerError)
		return
	}
	defer repairResp.Body.Close()

	var repair RepairModel
	if err := json.NewDecoder(repairResp.Body).Decode(&repair); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to decode repair for broadcasting")
		log.Printf("Failed to decode repair for broadcasting: %v", err)
		http.Error(w, "Failed to decode repair for broadcasting", http.StatusInternalServerError)
		return
	}

	// Broadcast status update to clients
	update := StatusUpdate{
		RepairID: repairID,
		UserID:   repair.UserID,
		Status:   input.Status,
	}
	h.broadcastStatusUpdate(update)

	w.WriteHeader(resp.StatusCode)
}

// HandleWebSocket manages WebSocket connections
func (h *RepairHandler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	_, span := h.tracer.Start(r.Context(), "HandleWebSocket")
	defer span.End()

	userID := r.URL.Query().Get("userID")
	if userID == "" {
		span.RecordError(fmt.Errorf("userID is required"))
		span.SetStatus(codes.Error, "userID is required")
		http.Error(w, "userID is required", http.StatusBadRequest)
		return
	}
	span.SetAttributes(attribute.String("userID", userID))

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to upgrade to WebSocket")
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

	// Keep connection alive
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			span.RecordError(err)
			log.Printf("WebSocket read error: %v", err)
			break
		}
	}
}

// broadcastStatusUpdate sends status updates to all clients subscribed to the userID
func (h *RepairHandler) broadcastStatusUpdate(update StatusUpdate) {
	_, span := h.tracer.Start(context.Background(), "BroadcastStatusUpdate")
	defer span.End()
	span.SetAttributes(
		attribute.String("repairID", update.RepairID),
		attribute.String("userID", update.UserID),
		attribute.String("status", update.Status),
	)

	h.clientsMutex.Lock()
	defer h.clientsMutex.Unlock()

	clients, exists := h.clients[update.UserID]
	if !exists {
		return
	}

	message, err := json.Marshal(update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to marshal status update")
		log.Printf("Failed to marshal status update: %v", err)
		return
	}

	for _, conn := range clients {
		err := conn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			span.RecordError(err)
			log.Printf("Failed to send WebSocket message: %v", err)
			conn.Close()
		}
	}
}
