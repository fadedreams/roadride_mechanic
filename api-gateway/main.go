package main

import (
	"api-gateway/handlers"
	"github.com/gorilla/mux"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"log"
	"net/http"
)

func main() {
	// Initialize handler
	repairHandler := handlers.NewRepairHandler()

	// Initialize router
	r := mux.NewRouter()

	// Add OpenTelemetry middleware
	r.Use(otelmux.Middleware("api-gateway"))

	// Define endpoints
	r.HandleFunc("/health", repairHandler.HealthCheck).Methods("GET")
	r.HandleFunc("/repairs", repairHandler.CreateRepair).Methods("POST")
	r.HandleFunc("/repairs/estimate", repairHandler.EstimateRepairCost).Methods("POST")
	r.HandleFunc("/repairs/cost/{costID}", repairHandler.GetRepairCost).Methods("GET")
	r.HandleFunc("/repairs/{repairID}", repairHandler.GetRepair).Methods("GET")
	r.HandleFunc("/repairs/{repairID}", repairHandler.UpdateRepair).Methods("PUT")
	r.HandleFunc("/ws", repairHandler.HandleWebSocket).Methods("GET")

	// Start server
	log.Println("API Gateway running on port 8081")
	if err := http.ListenAndServe(":8081", r); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
