package main

import (
	"context"
	"fmt"
	"github.com/gorilla/mux"
	"api-gateway/handlers"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	// Initialize tracer
	shutdown, err := initTracer()
	if err != nil {
		log.Fatalf("Failed to initialize tracer: %v", err)
	}
	defer shutdown()

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
    r.HandleFunc("/repairs/nearby", repairHandler.ListNearbyRepairs).Methods("GET") // Moved before /repairs/{repairID}
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

func initTracer() (func(), error) {
	jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "http://jaeger:4318/v1/traces"
	}
	log.Printf("Initializing tracer with Jaeger endpoint: %s", jaegerEndpoint)

	// Create OTLP exporter
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint("jaeger:4318"),
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithURLPath("/v1/traces"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %v", err)
	}

	// Test Jaeger connectivity with a GET request to the UI health endpoint
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://jaeger:16686/")
	if err != nil {
		log.Printf("Failed to connect to Jaeger UI (health check): %v", err)
	} else {
		log.Printf("Jaeger UI health check: status %d", resp.StatusCode)
		resp.Body.Close()
	}

	resources := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("api-gateway"),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter, sdktrace.WithExportTimeout(5*time.Second))),
		sdktrace.WithResource(resources),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	// Force a test span to verify export
	ctx := context.Background()
	tr := otel.Tracer("api-gateway")
	_, span := tr.Start(ctx, "TestSpan")
	span.SetAttributes(attribute.String("test", "true"))
	span.End()

	// Force export
	if err := tp.ForceFlush(ctx); err != nil {
		log.Printf("Failed to flush test span: %v", err)
	} else {
		log.Printf("Test span flushed successfully")
	}

	return func() {
		log.Printf("Shutting down tracer provider")
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}, nil
}
