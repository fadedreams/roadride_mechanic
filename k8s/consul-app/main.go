package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"log/slog"

	"github.com/gorilla/mux"
	"github.com/hashicorp/consul/api"
)

func main() {
	// Initialize structured logging (simplified for this example)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Log startup
	logger.Info("Starting consul-app", "app", "consul-app", "timestamp", time.Now().Unix())

	// Initialize Consul client and register service
	consulAddr := os.Getenv("CONSUL_ADDRESS")
	if consulAddr == "" {
		consulAddr = "consul-server.roadride.svc.cluster.local:8500"
	}
	consulConfig := api.DefaultConfig()
	consulConfig.Address = consulAddr
	consulClient, err := api.NewClient(consulConfig)
	if err != nil {
		logger.Error("Failed to create Consul client", "error", err, "app", "consul-app")
		os.Exit(1)
	}

	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = "consul-app"
	}
	servicePort := os.Getenv("SERVICE_PORT")
	if servicePort == "" {
		servicePort = "8089"
	}
	serviceID := fmt.Sprintf("%s-%s", serviceName, os.Getenv("HOSTNAME")) // Use pod hostname for unique ID
	registration := &api.AgentServiceRegistration{
		ID:      serviceID,
		Name:    serviceName,
		Port:    8089,
		Address: "localhost", // Use localhost since health check is internal
		Check: &api.AgentServiceCheck{
			HTTP:                           fmt.Sprintf("http://localhost:%s/health", servicePort),
			Interval:                       "10s",
			Timeout:                        "10s", // Increase timeout
			DeregisterCriticalServiceAfter: "5m", // Increase to 5 minutes
		},
	}
	if err := consulClient.Agent().ServiceRegister(registration); err != nil {
		logger.Error("Failed to register with Consul", "error", err, "app", "consul-app")
		os.Exit(1)
	}
	logger.Info("Registered with Consul", "service_id", serviceID, "app", "consul-app")

	// Initialize router
	r := mux.NewRouter()

	// Define endpoints
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status": "ok"}`))
	}).Methods("GET")
	r.HandleFunc("/consul-status", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf(`{"isRegistered": true, "serviceID": "%s"}`, serviceID)))
	}).Methods("GET")

	// Create HTTP server
	server := &http.Server{
		Addr:    ":" + servicePort,
		Handler: r,
	}

	// Start server in a goroutine
	go func() {
		logger.Info("Starting consul-app", "port", servicePort, "app", "consul-app")
		time.Sleep(5 * time.Second) // Add delay to ensure readiness
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Failed to start server", "error", err, "app", "consul-app")
			os.Exit(1)
		}
	}()
	// Handle graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("Received shutdown signal, shutting down gracefully", "app", "consul-app")

	// Create a context with timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Shutdown the HTTP server
	if err := server.Shutdown(ctx); err != nil {
		logger.Error("Failed to shutdown server", "error", err, "app", "consul-app")
	}

	// Deregister from Consul during shutdown
	if err := consulClient.Agent().ServiceDeregister(serviceID); err != nil {
		logger.Error("Failed to deregister from Consul", "error", err, "app", "consul-app")
	}
	logger.Info("Service shutdown complete", "app", "consul-app")
}
