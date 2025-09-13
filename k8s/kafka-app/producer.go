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
	topic := "test-topic"
	client := srclient.CreateSchemaRegistryClient("http://localhost:8081")

	schemaBytes, err := os.ReadFile("user.avsc")
	if err != nil {
		panic(fmt.Sprintf("Failed to read schema file: %v", err))
	}
	schemaStr := string(schemaBytes)
	fmt.Printf("Loaded schema: %s\n", schemaStr)

	schema, err := avro.Parse(schemaStr)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse schema: %v", err))
	}

	schemaObj, err := client.CreateSchema(topic+"-value", schemaStr, srclient.Avro)
	if err != nil {
		panic(fmt.Sprintf("Failed to register schema: %v", err))
	}
	schemaID := schemaObj.ID()
	fmt.Printf("Schema registered successfully with ID: %d\n", schemaID)

	clientPassword := os.Getenv("CLIENT_PASSWORD")
	if clientPassword == "" {
		panic("CLIENT_PASSWORD environment variable not set")
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:30092", // Updated to match EXTERNAL listener
		"security.protocol":  "SASL_PLAINTEXT",
		"sasl.mechanism":     "PLAIN",
		"sasl.username":      "user1",
		"sasl.password":      clientPassword,
		"compression.type":   "snappy",
		"enable.idempotence": "true",
		"acks":               "all",
		"retries":            "3",
		"debug":              "all", // Keep debug logging
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create producer: %v", err))
	}
	defer p.Close()

	user := User{Name: "John Doe", Age: 30}
	fmt.Printf("Serializing user: %+v\n", user)

	payload, err := avro.Marshal(schema, &user)
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize payload: %v", err))
	}
	fmt.Printf("Serialized payload: %v\n", payload)

	encodedPayload := make([]byte, 5+len(payload))
	encodedPayload[0] = 0
	binary.BigEndian.PutUint32(encodedPayload[1:5], uint32(schemaID))
	copy(encodedPayload[5:], payload)

	deliveryChan := make(chan kafka.Event)

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          encodedPayload,
	}, deliveryChan)
	if err != nil {
		panic(fmt.Sprintf("Failed to produce message: %v", err))
	}

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
