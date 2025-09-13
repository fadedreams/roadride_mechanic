package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
)

type User struct {
	Name string `avro:"name"`
	Age  int    `avro:"age"`
}

func main() {
	topic := "test-topic"

	// Schema Registry client
	client := srclient.CreateSchemaRegistryClient("http://localhost:8081")

	// Retrieve the client password from the Kubernetes secret
	clientPassword := os.Getenv("CLIENT_PASSWORD")
	if clientPassword == "" {
		panic("CLIENT_PASSWORD environment variable not set")
	}

	// Kafka consumer with SASL_PLAINTEXT
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"security.protocol":  "SASL_PLAINTEXT",
		"sasl.mechanism":     "PLAIN",
		"sasl.username":      "user1",
		"sasl.password":      clientPassword,
		"group.id":           "myGroup",
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %v", err))
	}
	defer c.Close()

	// Subscribe to topic
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to subscribe to topic: %v", err))
	}

	// Handle signals for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Consumer started. Press Ctrl+C to stop.")

	for {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			return
		default:
			msg, err := c.ReadMessage(-1)
			if err == nil {
				// Extract schema ID from payload (first 5 bytes: magic byte + 4-byte schema ID)
				if len(msg.Value) < 5 {
					fmt.Printf("Invalid message: too short\n")
					continue
				}
				if msg.Value[0] != 0 { // Check magic byte
					fmt.Printf("Invalid message: incorrect magic byte\n")
					continue
				}
				schemaID := binary.BigEndian.Uint32(msg.Value[1:5])

				// Retrieve schema from Schema Registry
				schemaObj, err := client.GetSchema(int(schemaID))
				if err != nil {
					fmt.Printf("Failed to retrieve schema with ID %d: %v\n", schemaID, err)
					continue
				}
				schemaStr := schemaObj.Schema()
				remoteSchema, err := avro.Parse(schemaStr)
				if err != nil {
					fmt.Printf("Failed to parse schema from registry: %v\n", err)
					continue
				}

				// Deserialize Avro payload
				var user User
				err = avro.Unmarshal(remoteSchema, msg.Value[5:], &user)
				if err != nil {
					fmt.Printf("Failed to deserialize message: %v\n", err)
					continue
				}
				fmt.Printf("Received message - Name: %s, Age: %d\n", user.Name, user.Age)
			} else {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue // Timeout, keep polling
				}
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
				return
			}
		}
	}
}

