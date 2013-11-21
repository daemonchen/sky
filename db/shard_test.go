package db

import (
	"fmt"
	"github.com/skydb/sky/core"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestShardStat(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-02T00:00:00Z", 1, "xxx"), false)
		s.InsertEvent("tbl0", "obj0", testevent("2000-01-01T00:00:00Z", 1, "yyy"), false)
		s.InsertEvent("tbl0", "obj1", testevent("2000-01-03T00:00:00Z", 1, "zzz"), false)
		s.InsertEvent("tbl1", "obj0", testevent("2000-01-05T00:00:00Z", 1, "zzz"), false)
		s.InsertEvent("tbl1", "obj0", testevent("2000-01-06T00:00:00Z", 1, "zzz"), false)
		stat, err := s.Stat()
		assert.Nil(t, err, "")
		assert.Equal(t, stat.Entries, uint64(2))
		assert.Equal(t, stat.Size, uint64(0x20000000000))
		assert.Equal(t, stat.Depth, uint(1))
		assert.Equal(t, stat.Transactions.Last, uint64(7))
		assert.Equal(t, stat.Readers.Max, uint(126))
		assert.Equal(t, stat.Readers.Current, uint(0))
		assert.Equal(t, stat.Pages.Last, uint64(10))
		assert.Equal(t, stat.Pages.Size, uint(0x1000))
		assert.Equal(t, stat.Pages.Branch, uint64(0))
		assert.Equal(t, stat.Pages.Leaf, uint64(1))
		assert.Equal(t, stat.Pages.Overflow, uint64(0))
	})
}

func withShard(f func(*shard)) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	s := newShard(path)
	if err := s.Open(4096, 126, 0); err != nil {
		panic(err.Error())
	}
	defer s.Close()

	f(s)
}

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
