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
	client := srclient.CreateSchemaRegistryClient("http://localhost:8081")

	clientPassword := os.Getenv("CLIENT_PASSWORD")
	if clientPassword == "" {
		panic("CLIENT_PASSWORD environment variable not set")
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:30092", // Updated to match EXTERNAL listener
		"security.protocol":  "SASL_PLAINTEXT",
		"sasl.mechanism":     "PLAIN",
		"sasl.username":      "user1",
		"sasl.password":      clientPassword,
		"group.id":           "myGroup",
		"auto.offset.reset":  "earliest",
		"debug":              "all", // Keep debug logging
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %v", err))
	}
	defer c.Close()

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to subscribe to topic: %v", err))
	}

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
				if len(msg.Value) < 5 {
					fmt.Printf("Invalid message: too short\n")
					continue
				}
				if msg.Value[0] != 0 {
					fmt.Printf("Invalid message: incorrect magic byte\n")
					continue
				}
				schemaID := binary.BigEndian.Uint32(msg.Value[1:5])

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

				var user User
				err = avro.Unmarshal(remoteSchema, msg.Value[5:], &user)
				if err != nil {
					fmt.Printf("Failed to deserialize message: %v\n", err)
					continue
				}
				fmt.Printf("Received message - Name: %s, Age: %d\n", user.Name, user.Age)
			} else {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
				return
			}
		}
	}
}
