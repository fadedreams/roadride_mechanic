package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

type LogEntry struct {
	Timestamp string `json:"@timestamp"`
	App       string `json:"app"`
	Message   string `json:"message"`
	Level     string `json:"level"`
}

func main() {
	// Initialize logger to write to /var/log/api-gateway/api-gateway.log
	logFile, err := os.OpenFile("/var/log/api-gateway/api-gateway.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()
	logger := log.New(logFile, "", 0)

	// Get Elasticsearch host from environment variable or default
	esHost := os.Getenv("ELASTICSEARCH_HOST")
	if esHost == "" {
		esHost = "http://elasticsearch-master:9200"
	}

	// Initialize Elasticsearch client with retry logic
	var es *elasticsearch.Client
	cfg := elasticsearch.Config{
		Addresses: []string{esHost},
	}
	for retries := 5; retries > 0; retries-- {
		es, err = elasticsearch.NewClient(cfg)
		if err == nil {
			break
		}
		logger.Printf(`{"@timestamp":"%s","app":"api-gateway","message":"Failed to create Elasticsearch client: %v, retrying...","level":"ERROR"}`, time.Now().Format(time.RFC3339), err)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		logger.Printf(`{"@timestamp":"%s","app":"api-gateway","message":"Failed to create Elasticsearch client after retries: %v","level":"ERROR"}`, time.Now().Format(time.RFC3339), err)
		log.Fatalf("Failed to create Elasticsearch client: %v", err)
	}

	// Test Elasticsearch connection
	res, err := es.Info()
	if err != nil {
		logger.Printf(`{"@timestamp":"%s","app":"api-gateway","message":"Error getting Elasticsearch info: %v","level":"ERROR"}`, time.Now().Format(time.RFC3339), err)
		log.Fatalf("Error getting Elasticsearch info: %v", err)
	}
	defer res.Body.Close()
	logger.Printf(`{"@timestamp":"%s","app":"api-gateway","message":"Connected to Elasticsearch: %s","level":"INFO"}`, time.Now().Format(time.RFC3339), res.String())

	// Start a goroutine to log a sample message every 10 seconds
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				logEntry := LogEntry{
					Timestamp: time.Now().Format(time.RFC3339),
					App:       "api-gateway",
					Message:   "Periodic sample log",
					Level:     "INFO",
				}
				logJSON, _ := json.Marshal(logEntry)
				logger.Println(string(logJSON))
			}
		}
	}()

	// HTTP handler for /log endpoint
	http.HandleFunc("/log", func(w http.ResponseWriter, r *http.Request) {
		// Log request
		logEntry := LogEntry{
			Timestamp: time.Now().Format(time.RFC3339),
			App:       "api-gateway",
			Message:   "Received request to /log",
			Level:     "INFO",
		}
		logJSON, _ := json.Marshal(logEntry)
		logger.Println(string(logJSON))

		// Index a sample document in Elasticsearch
		doc := map[string]interface{}{
			"timestamp": time.Now().Format(time.RFC3339),
			"message":   "Sample log from Go app",
			"app":       "api-gateway",
		}
		docJSON, _ := json.Marshal(doc)
		res, err := es.Index("apigateway-logs", bytes.NewReader(docJSON), es.Index.WithContext(context.Background()))
		if err != nil {
			logger.Printf(`{"@timestamp":"%s","app":"api-gateway","message":"Failed to index document: %v","level":"ERROR"}`, time.Now().Format(time.RFC3339), err)
			http.Error(w, "Failed to index document", http.StatusInternalServerError)
			return
		}
		defer res.Body.Close()
		logger.Printf(`{"@timestamp":"%s","app":"api-gateway","message":"Indexed document: %s","level":"INFO"}`, time.Now().Format(time.RFC3339), res.String())

		fmt.Fprintf(w, "Log written and indexed in Elasticsearch")
	})

	// HTTP handler for /health endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		logEntry := LogEntry{
			Timestamp: time.Now().Format(time.RFC3339),
			App:       "api-gateway",
			Message:   "Health check OK",
			Level:     "INFO",
		}
		logJSON, _ := json.Marshal(logEntry)
		logger.Println(string(logJSON))
		fmt.Fprintf(w, "OK")
	})

	// Start HTTP server on port 8085
	logger.Printf(`{"@timestamp":"%s","app":"api-gateway","message":"Starting server on :8085","level":"INFO"}`, time.Now().Format(time.RFC3339))
	log.Fatal(http.ListenAndServe(":8085", nil))
}
