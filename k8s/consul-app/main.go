package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/hashicorp/consul/api"
)

type RegistrationStatus struct {
	IsRegistered bool   `json:"isRegistered"`
	ServiceID    string `json:"serviceID"`
	Error        string `json:"error,omitempty"`
}

func main() {
	// Get Consul address from environment variable
	consulAddress := os.Getenv("CONSUL_ADDRESS")
	if consulAddress == "" {
		log.Fatal("CONSUL_ADDRESS environment variable not set")
	}

	// Initialize Consul client
	config := api.DefaultConfig()
	config.Address = consulAddress
	client, err := api.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create Consul client: %v", err)
	}

	// Service details
	serviceID := "consul-app-" + os.Getenv("HOSTNAME")
	serviceName := "consul-app"
	servicePort := 8089
	host := "localhost"

	// Get port from environment or default to 8089
	portEnv := os.Getenv("PORT")
	if portEnv != "" {
		fmt.Sscanf(portEnv, "%d", &servicePort)
	}

	// Register service with Consul
	registration := &api.AgentServiceRegistration{
		ID:      serviceID,
		Name:    serviceName,
		Address: host,
		Port:    servicePort,
		Checks: []*api.AgentServiceCheck{
			{
				HTTP:                           fmt.Sprintf("http://%s:%d/health", host, servicePort),
				Interval:                       "10s",
				Timeout:                        "5s",
				DeregisterCriticalServiceAfter: "30s",
			},
		},
	}

	var registrationStatus RegistrationStatus
	registrationStatus.ServiceID = serviceID

	// Attempt to register with Consul
	err = client.Agent().ServiceRegister(registration)
	if err != nil {
		log.Printf("Failed to register with Consul: %v", err)
		registrationStatus.IsRegistered = false
		registrationStatus.Error = err.Error()
	} else {
		log.Printf("Successfully registered with Consul as %s", serviceID)
		registrationStatus.IsRegistered = true
	}

	// Deregister service on exit
	defer func() {
		if err := client.Agent().ServiceDeregister(serviceID); err != nil {
			log.Printf("Failed to deregister service %s: %v", serviceID, err)
		} else {
			log.Printf("Deregistered service %s from Consul", serviceID)
		}
	}()

	// Health endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status": "ok"}`)
	})

	// Consul registration status endpoint
	http.HandleFunc("/consul-status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if registrationStatus.IsRegistered {
			fmt.Fprintf(w, `{"isRegistered": true, "serviceID": "%s"}`, registrationStatus.ServiceID)
		} else {
			fmt.Fprintf(w, `{"isRegistered": false, "serviceID": "%s", "error": "%s"}`, 
				registrationStatus.ServiceID, registrationStatus.Error)
		}
	})

	// Start HTTP server
	log.Printf("Starting server on port %d", servicePort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", servicePort), nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}
