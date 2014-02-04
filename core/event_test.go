package core

import (
	"bytes"
	"testing"
	"time"
)

// Encode and then decode an Event to MsgPack.
func TestEncodeDecode(t *testing.T) {
	timeString := "1970-01-01T00:01:00.123456Z"
	timestamp, err := time.Parse(time.RFC3339, timeString)
	e1 := &Event{Timestamp: timestamp}
	e1.Data = map[int64]interface{}{-1: int64(20), -2: "baz", 10: int64(123)}

	// Encode
	buffer := new(bytes.Buffer)
	err = e1.EncodeRaw(buffer)
	if err != nil {
		t.Fatalf("Unable to encode: %v", err)
	}

	// Decode
	e2 := &Event{}
	err = e2.DecodeRaw(buffer)
	if err != nil {
		t.Fatalf("Unable to decode: %v", err)
	}
	if !e1.Equal(e2) {
		t.Fatalf("Events do not match: %v <=> %v", e1, e2)
	}
}

// Ensure that two events can be merged together.
func TestMerge(t *testing.T) {
	a := NewEvent("1970-01-01T00:00:00Z", map[int64]interface{}{-1: int64(30), -2: "foo"})
	b := NewEvent("1970-01-01T00:00:00Z", map[int64]interface{}{-1: 20, 3: "baz"})
	a.Merge(b)
	if a.Data[-1] != 20 || a.Data[-2] != "foo" || a.Data[3] != "baz" {
		t.Fatalf("Invalid merge: %v", a.Data)
	}
}

// Ensure that permanent values of two events can be merged together.
func TestMergePermanent(t *testing.T) {
	a := NewEvent("1970-01-01T00:00:00Z", map[int64]interface{}{-1: int64(30), -2: "foo"})
	b := NewEvent("1970-01-01T00:00:00Z", map[int64]interface{}{-1: 20, -10: "bat", 3: "baz"})
	a.MergePermanent(b)
	if a.Data[-1] != int64(30) || a.Data[-2] != "foo" || a.Data[-10] != nil || a.Data[3] != "baz" {
		t.Fatalf("Invalid permanent merge: %v", a.Data)
	}
}

// Ensure that duplicate values of two events can be deduplicated.
func TestDedupe(t *testing.T) {
	a := NewEvent("1970-01-01T00:00:00Z", map[int64]interface{}{1: int64(20), 2: "foo", 3: "baz"})
	b := NewEvent("1970-01-01T00:00:00Z", map[int64]interface{}{1: int32(20), 2: "bar", 3: "baz"})
	a.DedupePermanent(b)
	if a.Data[1] != nil || a.Data[2] != "foo" || a.Data[3] != nil {
		t.Fatalf("Invalid dedupe: %v", a.Data)
	}
}

// Ensure that EventList is duplicate values of two events can be deduplicated.
func TestEventListDeduping(t *testing.T) {
	a := NewEvent("1980-01-01T00:00:00Z", map[int64]interface{}{1: 1, 2: "foo", 3: "baz"})
	b := NewEvent("1980-01-01T00:00:00Z", map[int64]interface{}{1: 2, 2: "bar", 3: "baz"})
	c := NewEvent("1970-01-01T00:00:00Z", map[int64]interface{}{1: 3, 2: "foo", 3: "baz"})
	d := NewEvent("1990-01-01T00:00:00Z", map[int64]interface{}{1: 4, 2: "foo", 3: "baz"})
	e := NewEvent("1980-01-01T00:00:00Z", map[int64]interface{}{1: 5, 2: "baz", 3: "baz"})
	f := NewEvent("1990-01-01T00:00:00Z", map[int64]interface{}{1: 6, 2: "bar", 3: "baz"})
	list := EventList([]*Event{a, b, c, d, e, f}).Normalize().Sort().Dedupe()
	expected := []*Event{c, a, d}

	if len(list) != len(expected) {
		t.Fatalf("Wrong merged list size: %v", len(list))
	}

	for index, event := range expected {
		if list[index] != event {
			t.Fatalf("Wrong item %d in the merged list: %v", index, list[index])
		}
	}
}

// Ensure that EventList is duplicate values of two events can be deduplicated.
func TestEventListMerging(t *testing.T) {
	a := NewEvent("1970-01-01T00:00:00Z", map[int64]interface{}{1: 1})
	b := NewEvent("1980-01-01T00:00:00Z", map[int64]interface{}{1: 2})
	c := NewEvent("1990-01-01T00:00:00Z", map[int64]interface{}{1: 3})
	d := NewEvent("1975-01-01T00:00:00Z", map[int64]interface{}{1: 4})
	e := NewEvent("1980-01-01T00:00:00Z", map[int64]interface{}{1: 5})
	f := NewEvent("1985-01-01T00:00:00Z", map[int64]interface{}{1: 6})
	new := []*Event{a, b, c}
	old := []*Event{d, e, f}
	expected := []*Event{a, d, b, f, c}

	merged := EventList(new).Merge(old)

	if len(merged) != len(expected) {
		t.Fatalf("Wrong merged list size: %v", len(merged))
	}

	for index, event := range expected {
		if merged[index] != event {
			t.Fatalf("Wrong item %d in the merged list: %v", index, merged[index])
		}
	}
}
