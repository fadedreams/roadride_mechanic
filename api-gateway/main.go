package main

import (
	"api-gateway/handlers"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

func main() {
	// Initialize router
	r := mux.NewRouter()

	// Initialize repair handler with the repair-service URL
	repairHandler := handlers.NewRepairHandler("http://repair-service:8080")

	// Define API endpoints
	r.HandleFunc("/repairs", repairHandler.CreateRepair).Methods("POST")
	r.HandleFunc("/repairs/estimate", repairHandler.EstimateRepairCost).Methods("POST")
	r.HandleFunc("/repairs/cost/{costID}", repairHandler.GetRepairCost).Methods("GET")
	r.HandleFunc("/repairs/{repairID}", repairHandler.GetRepair).Methods("GET")
	r.HandleFunc("/repairs/{repairID}", repairHandler.UpdateRepair).Methods("PUT")

	// Start server
	log.Println("API Gateway running on port 8081")
	if err := http.ListenAndServe(":8081", r); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
