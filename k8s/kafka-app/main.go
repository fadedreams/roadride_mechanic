package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "time"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
    "github.com/gorilla/mux"
)

// Avro schema structs (matches repair.service.RepairEvent)
type Location struct {
    Longitude float64 `json:"longitude"`
    Latitude  float64 `json:"latitude"`
}

type MechanicInfo struct {
    ID       string   `json:"id"`
    Name     string   `json:"name"`
    Location Location `json:"location"`
    Distance float64  `json:"distance"`
}

type RepairEvent struct {
    ID           string        `json:"id"`
    UserID       string        `json:"user_id"`
    Status       string        `json:"status"`
    RepairType   string        `json:"repair_type"`
    TotalPrice   float64       `json:"total_price"`
    UserLocation Location      `json:"user_location"`
    Mechanics    []MechanicInfo `json:"mechanics"`
}

func main() {
    // Initialize Schema Registry client
    srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(os.Getenv("SCHEMA_REGISTRY_URL")))
    if err != nil {
        log.Fatalf("Failed to create Schema Registry client: %v", err)
    }
    defer srClient.Close()

    // Avro serializer
    avroConf := avro.NewSerializerConfig()
    avroConf.AutoRegisterSchemas = false // Use existing schema ID
    serializer, err := avro.NewSpecificSerializer(srClient, serde.ValueSerde, avroConf)
    if err != nil {
        log.Fatalf("Failed to create serializer: %v", err)
    }

    // Kafka producer
    producer, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
    })
    if err != nil {
        log.Fatalf("Failed to create producer: %v", err)
    }
    defer producer.Close()

    // Kafka consumer (for demo; subscribes to repair_events)
    consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers":  os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "group.id":           "kafka-app-group",
        "auto.offset.reset":  "earliest",
    })
    if err != nil {
        log.Fatalf("Failed to create consumer: %v", err)
    }
    defer consumer.Close()
    err = consumer.SubscribeTopics([]string{"repair_events"}, nil)
    if err != nil {
        log.Fatalf("Failed to subscribe: %v", err)
    }

    // HTTP router
    r := mux.NewRouter()

    // Health endpoint
    r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        fmt.Fprintf(w, `{"status": "healthy", "timestamp": "%s"}`, time.Now().Format(time.RFC3339))
    })

    // Produce endpoint
    r.HandleFunc("/produce-repair-event", func(w http.ResponseWriter, r *http.Request) {
        event := RepairEvent{
            ID:         "evt-" + time.Now().Format("20060102150405"),
            UserID:     "user1",
            Status:     "pending",
            RepairType: "flat_tire",
            TotalPrice: 50.0,
            UserLocation: Location{Longitude: 13.4, Latitude: 52.52},
            Mechanics: []MechanicInfo{
                {ID: "m1", Name: "John Doe", Location: Location{Longitude: 13.5, Latitude: 52.5}, Distance: 1.2},
            },
        }

        // Serialize to Avro (schema ID 1 from registration)
        payload, err := serializer.Serialize("repair.service+RepairEvent-value", &event)
        if err != nil {
            http.Error(w, fmt.Sprintf("Serialization error: %v", err), http.StatusInternalServerError)
            return
        }

        // Produce
        topic := "repair_events"
        deliveryChan := make(chan kafka.Event)
        err = producer.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            Value:          payload,
        }, deliveryChan)
        if err != nil {
            http.Error(w, fmt.Sprintf("Produce error: %v", err), http.StatusInternalServerError)
            return
        }

        // Confirm delivery
        e := <-deliveryChan
        m := e.(*kafka.Message)
        if m.TopicPartition.Error != nil {
            http.Error(w, fmt.Sprintf("Delivery failed: %v", m.TopicPartition.Error), http.StatusInternalServerError)
            return
        }

        w.WriteHeader(http.StatusOK)
        fmt.Fprintf(w, `{"status": "produced", "schema_id": 1, "topic": "%s"}`, topic)
    })

    // Consumer loop (runs in background; logs messages to stdout)
    go func() {
        for {
            msg, err := consumer.ReadMessage(time.Second * 10)
            if err != nil {
                if !err.(kafka.Error).IsTimeout() {
                    log.Printf("Consumer error: %v\n", err)
                }
                continue
            }
            // Deserialize (expects Avro with schema ID 1)
            var event RepairEvent
            err = serializer.Deserialize("repair.service+RepairEvent-value", msg.Value, &event)
            if err != nil {
                log.Printf("Deserialization error: %v\n", err)
                continue
            }
            log.Printf("Consumed event: %+v\n", event)
        }
    }()

    // Start server
    log.Println("Kafka app starting on :8087")
    log.Fatal(http.ListenAndServe(":8087", r))
}
