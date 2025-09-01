package main

import (
	"api-gateway/handlers"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// multiHandler is a custom slog.Handler that combines multiple handlers
type multiHandler []slog.Handler

func (h multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h multiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, handler := range h {
		if handler.Enabled(ctx, r.Level) {
			if err := handler.Handle(ctx, r); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(h))
	for i, handler := range h {
		handlers[i] = handler.WithAttrs(attrs)
	}
	return multiHandler(handlers)
}

func (h multiHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(h))
	for i, handler := range h {
		handlers[i] = handler.WithGroup(name)
	}
	return multiHandler(handlers)
}

func main() {
	// Initialize structured logging for both file and terminal
	logFile, err := os.OpenFile("/var/log/api-gateway/api-gateway.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("Failed to open log file", "error", err)
		os.Exit(1)
	}
	defer logFile.Close()

	// Create handlers for both file and terminal output
	fileHandler := slog.NewJSONHandler(logFile, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
	})
	terminalHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
	})

	// Combine handlers using custom multiHandler
	logger := slog.New(multiHandler{fileHandler, terminalHandler})
	slog.SetDefault(logger)

	// Log startup
	slog.Info("Starting API Gateway", "app", "api-gateway", "timestamp", time.Now().Unix())

	// Initialize tracer
	shutdown, err := initTracer()
	if err != nil {
		slog.Error("Failed to initialize tracer", "error", err)
		os.Exit(1)
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
	r.HandleFunc("/repairs/nearby", repairHandler.ListNearbyRepairs).Methods("GET")
	r.HandleFunc("/repairs/cost/{costID}", repairHandler.GetRepairCost).Methods("GET")
	r.HandleFunc("/repairs/{repairID}", repairHandler.GetRepair).Methods("GET")
	r.HandleFunc("/repairs/{repairID}", repairHandler.UpdateRepair).Methods("PUT")
	r.HandleFunc("/ws", repairHandler.HandleWebSocket).Methods("GET")

	// Start server
	slog.Info("API Gateway running on port 8081")
	if err := http.ListenAndServe(":8081", r); err != nil {
		slog.Error("Failed to start server", "error", err)
		os.Exit(1)
	}
}

func initTracer() (func(), error) {
	jaegerEndpoint := os.Getenv("JAEGER_ENDPOINT")
	if jaegerEndpoint == "" {
		jaegerEndpoint = "http://jaeger:4318/v1/traces"
	}
	slog.Info("Initializing tracer", "jaeger_endpoint", jaegerEndpoint)

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
		slog.Error("Failed to connect to Jaeger UI (health check)", "error", err)
	} else {
		slog.Info("Jaeger UI health check", "status_code", resp.StatusCode)
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
		slog.Error("Failed to flush test span", "error", err)
	} else {
		slog.Info("Test span flushed successfully")
	}

	return func() {
		slog.Info("Shutting down tracer provider")
		if err := tp.Shutdown(context.Background()); err != nil {
			slog.Error("Error shutting down tracer provider", "error", err)
		}
	}, nil
}
