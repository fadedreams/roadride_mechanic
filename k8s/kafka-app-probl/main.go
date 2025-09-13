package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
)

// RepairEvent matches the repair_event.avsc schema
type RepairEvent struct {
	ID           string        `avro:"id"`
	UserID       string        `avro:"user_id"`
	Status       string        `avro:"status"`
	RepairType   string        `avro:"repair_type"`
	TotalPrice   float64       `avro:"total_price"`
	UserLocation Location      `avro:"user_location"`
	Mechanics    []MechanicInfo `avro:"mechanics"`
}

type Location struct {
	Longitude float64 `avro:"longitude"`
	Latitude  float64 `avro:"latitude"`
}

type MechanicInfo struct {
	ID       string   `avro:"id"`
	Name     string   `avro:"name"`
	Location Location `avro:"location"`
	Distance float64  `avro:"distance"`
}

func main() {
	// Configuration
	topic := "repair-events"
	schemaRegistryURL := os.Getenv("SCHEMA_REGISTRY_URL")
	kafkaBootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	port := os.Getenv("PORT")
	if port == "" {
		port = "8087"
	}

	// Schema Registry client
	srClient := srclient.CreateSchemaRegistryClient(schemaRegistryURL)

	// Load Avro schema
	schemaBytes, err := os.ReadFile("repair_event.avsc")
	if err != nil {
		log.Fatalf("Failed to read schema file: %v", err)
	}
	schemaStr := string(schemaBytes)
	schema, err := avro.Parse(schemaStr)
	if err != nil {
		log.Fatalf("Failed to parse schema: %v", err)
	}

	// Register schema
	schemaObj, err := srClient.CreateSchema(topic+"-value", schemaStr, srclient.Avro)
	if err != nil {
		log.Fatalf("Failed to register schema: %v", err)
	}
	schemaID := schemaObj.ID()
	log.Printf("Schema registered with ID: %d", schemaID)

	// Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"compression.type":  "snappy",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"group.id":          "kafka-app-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %v", err)
	}

	// HTTP server for health and status
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status": "ok"}`)
	})
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"kafka_connected": true}`)
	})
	go func() {
		log.Printf("Starting HTTP server on port %s", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	// Produce sample message
	go func() {
		deliveryChan := make(chan kafka.Event)
		repairEvent := RepairEvent{
			ID:         "repair-001",
			UserID:     "user-001",
			Status:     "pending",
			RepairType: "flat_tire",
			TotalPrice: 99.99,
			UserLocation: Location{Longitude: 13.4, Latitude: 52.52},
			Mechanics: []MechanicInfo{
				{ID: "mech-001", Name: "John Doe", Location: Location{Longitude: 13.41, Latitude: 52.51}, Distance: 1.2},
			},
		}
		payload, err := avro.Marshal(schema, &repairEvent)
		if err != nil {
			log.Printf("Failed to serialize payload: %v", err)
			return
		}
		encodedPayload := make([]byte, 5+len(payload))
		encodedPayload[0] = 0
		binary.BigEndian.PutUint32(encodedPayload[1:5], uint32(schemaID))
		copy(encodedPayload[5:], payload)

		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          encodedPayload,
		}, deliveryChan)
		if err != nil {
			log.Printf("Failed to produce message: %v", err)
			return
		}

		e := <-deliveryChan
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			log.Printf("Delivery failed: %v", m.TopicPartition.Error)
		} else {
			log.Printf("Delivered message to topic %s [%d] at offset %v",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
		close(deliveryChan)
	}()

	// Consume messages and log them
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating", sig)
			return
		default:
			msg, err := consumer.ReadMessage(1 * time.Second)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Consumer error: %v", err)
				continue
			}
			if len(msg.Value) < 5 || msg.Value[0] != 0 {
				log.Printf("Invalid message format")
				continue
			}
			schemaID := binary.BigEndian.Uint32(msg.Value[1:5])
			schemaObj, err := srClient.GetSchema(int(schemaID))
			if err != nil {
				log.Printf("Failed to retrieve schema with ID %d: %v", schemaID, err)
				continue
			}
			schema, err := avro.Parse(schemaObj.Schema())
			if err != nil {
				log.Printf("Failed to parse schema: %v", err)
				continue
			}
			var event RepairEvent
			err = avro.Unmarshal(schema, msg.Value[5:], &event)
			if err != nil {
				log.Printf("Failed to deserialize message: %v", err)
				continue
			}
			log.Printf("Received RepairEvent: %+v", event)
		}
	}
}
