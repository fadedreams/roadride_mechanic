package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
)

type User struct {
	Name string `avro:"name"`
	Age  int    `avro:"age"`
}

func main() {
	// Topic to produce to (align with Minikube script)
	topic := "test-topic"

	// Schema Registry client
	client := srclient.CreateSchemaRegistryClient("http://localhost:8081")

	// Load the Avro schema from file
	schemaBytes, err := os.ReadFile("user.avsc")
	if err != nil {
		panic(fmt.Sprintf("Failed to read schema file: %v", err))
	}
	schemaStr := string(schemaBytes)
	fmt.Printf("Loaded schema: %s\n", schemaStr)

	// Parse the Avro schema for serialization
	schema, err := avro.Parse(schemaStr)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse schema: %v", err))
	}

	// Register the schema with Schema Registry
	schemaObj, err := client.CreateSchema(topic+"-value", schemaStr, srclient.Avro)
	if err != nil {
		panic(fmt.Sprintf("Failed to register schema: %v", err))
	}
	schemaID := schemaObj.ID()
	fmt.Printf("Schema registered successfully with ID: %d\n", schemaID)

	// Retrieve the client password from the Kubernetes secret
	clientPassword := os.Getenv("CLIENT_PASSWORD")
	if clientPassword == "" {
		panic("CLIENT_PASSWORD environment variable not set")
	}

	// Kafka producer with SASL_PLAINTEXT
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"security.protocol":  "SASL_PLAINTEXT",
		"sasl.mechanism":     "PLAIN",
		"sasl.username":      "user1",
		"sasl.password":      clientPassword,
		"compression.type":   "snappy",
		"enable.idempotence": "true",
		"acks":               "all",
		"retries":            "3",
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %v", err))
	}
	defer p.Close()

	// Sample message
	user := User{Name: "John Doe", Age: 30}
	fmt.Printf("Serializing user: %+v\n", user)

	// Serialize to Avro using hamba/avro
	payload, err := avro.Marshal(schema, &user)
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize payload: %v", err))
	}
	fmt.Printf("Serialized payload: %v\n", payload)

	// Add Schema Registry wire format: magic byte (0) + 4-byte schema ID
	encodedPayload := make([]byte, 5+len(payload))
	encodedPayload[0] = 0 // Magic byte
	binary.BigEndian.PutUint32(encodedPayload[1:5], uint32(schemaID))
	copy(encodedPayload[5:], payload)

	// Delivery report channel
	deliveryChan := make(chan kafka.Event)

	// Produce message
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          encodedPayload,
	}, deliveryChan)
	if err != nil {
		panic(fmt.Sprintf("Failed to produce message: %v", err))
	}

	// Wait for delivery report
	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)
}
