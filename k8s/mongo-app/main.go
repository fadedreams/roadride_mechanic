package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Mechanic struct {
	Name  string `bson:"name"`
	Skill string `bson:"skill"`
}

func main() {
	// Get MongoDB URI from environment variable
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("MONGO_URI environment variable not set")
	}

	// Set up MongoDB client options
	clientOptions := options.Client().ApplyURI(mongoURI).SetConnectTimeout(10 * time.Second)

	// Connect to MongoDB (v2 API: pass context directly)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Ping the MongoDB server
	if err = client.Ping(ctx, nil); err != nil {
		log.Fatalf("Failed to ping MongoDB: %v", err)
	}
	fmt.Println("Connected to MongoDB!")

	// Access the repairdb database and mechanics collection
	collection := client.Database("repairdb").Collection("mechanics")

	// Insert a sample mechanic document
	mechanic := Mechanic{Name: "John Doe", Skill: "Engine Repair"}
	insertResult, err := collection.InsertOne(ctx, mechanic)
	if err != nil {
		log.Fatalf("Failed to insert document: %v", err)
	}
	fmt.Printf("Inserted mechanic document with ID %v: %+v\n", insertResult.InsertedID, mechanic)

	// Query the collection for the inserted document
	var result Mechanic
	err = collection.FindOne(ctx, bson.M{"name": "John Doe"}).Decode(&result)
	if err != nil {
		log.Fatalf("Failed to query document: %v", err)
	}
	fmt.Printf("Found mechanic: %+v\n", result)

	// Keep the app running to allow Kubernetes liveness/readiness probes
	select {}
}
