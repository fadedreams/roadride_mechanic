package domain

import "time"

// Repair represents a repair request
type Repair struct {
	ID         string       `json:"id" bson:"_id"`
	UserID     string       `json:"userID" bson:"userID"`
	Status     string       `json:"status" bson:"status"`
	RepairCost *RepairCost  `json:"repairCost" bson:"repairCost"`
	AssignedTo string       `json:"assignedTo" bson:"assignedTo,omitempty"`
}

// RepairCost represents the cost details of a repair
type RepairCost struct {
	ID           string         `json:"id" bson:"_id"`
	UserID       string         `json:"userID" bson:"userID"`
	RepairType   string         `json:"repairType" bson:"repairType"`
	TotalPrice   float64        `json:"totalPrice" bson:"totalPrice"`
	UserLocation *Location      `json:"userLocation" bson:"userLocation,omitempty"`
	Mechanics    []MechanicInfo `json:"mechanics" bson:"mechanics,omitempty"`
}

// Location represents geographic coordinates
type Location struct {
	Latitude  float64 `json:"latitude" bson:"latitude"`
	Longitude float64 `json:"longitude" bson:"longitude"`
}

// Mechanic represents a mechanic
type Mechanic struct {
	ID       string   `json:"id" bson:"_id"`
	Name     string   `json:"name" bson:"name"`
	Location Location `json:"location" bson:"location"`
}

// MechanicInfo represents a mechanic with distance from user
type MechanicInfo struct {
	ID       string   `json:"id" bson:"id"`
	Name     string   `json:"name" bson:"name"`
	Location Location `json:"location" bson:"location"`
	Distance float64  `json:"distance" bson:"distance"`
}

// OutboxEvent represents an event in the outbox collection
type OutboxEvent struct {
	ID          string     `bson:"_id" json:"id"`
	EventType   string     `bson:"event_type" json:"event_type"`
	Payload     []byte     `bson:"payload" json:"payload"`
	CreatedAt   time.Time  `bson:"created_at" json:"created_at"`
	Processed   bool       `bson:"processed" json:"processed"`
	ProcessedAt *time.Time `bson:"processed_at" json:"processed_at,omitempty"`
}

