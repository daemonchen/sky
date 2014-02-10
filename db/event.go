package db

import (
	"time"
)

// Event represents the state for an object at a given point in time.
type Event struct {
	Timestamp time.Time
	Data      map[int64]interface{}
}

