package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Configuration for Kafka
	// bootstrapServers := "my-cluster-kafka-bootstrap:9092"
	bootstrapServers := "localhost:9094"
	topic := "my-topic"
	consumerGroup := "my-consumer-group"

	// WaitGroup to manage producer and consumer goroutines
	var wg sync.WaitGroup
	wg.Add(2)

	// Channel to handle OS signals for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Context for cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Start producer in a goroutine
	go func() {
		defer wg.Done()
		produceMessages(ctx, bootstrapServers, topic)
	}()

	// Start consumer in a goroutine
	go func() {
		defer wg.Done()
		consumeMessages(ctx, bootstrapServers, topic, consumerGroup)
	}()

	// Wait for interrupt signal to gracefully shutdown
	<-sigchan
	fmt.Println("\nReceived shutdown signal, closing connections...")
	cancel()

	// Wait for producer and consumer to finish
	wg.Wait()
	fmt.Println("Application shutdown complete.")
}

func produceMessages(ctx context.Context, bootstrapServers, topic string) {
	// Create producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages until context is cancelled
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping producer...")
			p.Flush(1000) // Wait for any outstanding messages
			return
		case <-ticker.C:
			message := fmt.Sprintf("Message from Go producer at %s", time.Now().Format(time.RFC3339))
			err := p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(message),
			}, nil)
			if err != nil {
				fmt.Printf("Failed to produce message: %v\n", err)
			}
		}
	}
}

func consumeMessages(ctx context.Context, bootstrapServers, topic, groupID string) {
	// Create consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer c.Close()

	// Subscribe to topic
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %v", err)
	}

	fmt.Printf("Consumer started, listening on topic: %s\n", topic)

	// Consume messages until context is cancelled
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping consumer...")
			return
		default:
			msg, err := c.ReadMessage(1 * time.Second)
			if err == nil {
				fmt.Printf("Received message: %s\n", string(msg.Value))
			} else if err.(kafka.Error).Code() != kafka.ErrTimedOut {
				fmt.Printf("Consumer error: %v\n", err)
			}
		}
	}
}
