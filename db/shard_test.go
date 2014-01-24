package db

import (
	"fmt"
	"github.com/skydb/sky/core"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func createEvent(timestamp time.Time, data map[int64]interface{}) *core.Event {
	return &core.Event{Timestamp: timestamp, Data: data}
}

// Ensure that dedupeEvents does as expected
func TestDedupeEvents(t *testing.T) {
	now := time.Now()
	otherNow := now.Add(1 * time.Second)
	events := []*core.Event{
		createEvent(now, map[int64]interface{}{1: "foo", 2: "bar", 3: 10}),
		createEvent(now, map[int64]interface{}{1: "foo", 3: 12}),
		createEvent(otherNow, map[int64]interface{}{1: "foo", 2: "bar"}),
	}
	dedupedEvents := dedupeEvents(events)
	assert.NotEqual(t, len(dedupedEvents), len(events), fmt.Sprintf("Events deduping failed: %+v - %+v", dedupedEvents, events))
	assert.Equal(t, len(dedupedEvents), 2)

	event1 := dedupedEvents[0]
	assert.Equal(t, event1.Timestamp, now)
	assert.Equal(t, event1.Data[1], "foo")
	assert.Equal(t, event1.Data[2], "bar")
	assert.Equal(t, event1.Data[3], 12)

	event2 := dedupedEvents[1]
	assert.Equal(t, event2.Timestamp, otherNow)
	assert.Equal(t, event2.Data[1], "foo")
	assert.Equal(t, event2.Data[2], "bar")
}
