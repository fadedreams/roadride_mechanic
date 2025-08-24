
package domain

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type RepairCostModel struct {
	ID                primitive.ObjectID
	UserID            string
	RepairType       string
	TotalPrice float64
}

type RepairModel struct {
	ID       primitive.ObjectID
	UserID   string
	Status   string
	RepairCost *RepairCostModel
}


