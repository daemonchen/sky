package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that we can put an event on the server.
func TestServerEventUpdate(t *testing.T) {
	runTestServer(func(s *Server) {
		var resp interface{}
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "factor")
		setupTestProperty("foo", "baz", true, "integer")

		// Send two new events.
		code, resp := putJSON("/tables/foo/objects/xyz/events/2012-01-01T02:00:00.123456111Z", `{"data":{"bar":"myValue", "baz":12}}`)
		assert.Equal(t, code, 200)
		code, _ = putJSON("/tables/foo/objects/xyz/events/2012-01-01T03:00:00Z", `{"data":{"bar":"myValue2"}}`)
		assert.Equal(t, code, 200)

		// Merge new events.
		code, _ = putJSON("/tables/foo/objects/xyz/events/2012-01-01T02:00:00.123456222Z", `{"data":{"bar":"myValue3", "baz":1000}}`)
		assert.Equal(t, code, 200)
		code, _ = putJSON("/tables/foo/objects/xyz/events/2012-01-01T03:00:00Z", `{"data":{"bar":"myValue2", "baz":20}}`)
		assert.Equal(t, code, 200)

		// Check the resulting events.
		code, resp = getJSON("/tables/foo/objects/xyz/events")
		assert.Equal(t, code, 200)
		if resp, ok := resp.([]interface{}); assert.True(t, ok) {
			assert.Equal(t, len(resp), 2)
			assert.Equal(t, jsonenc(resp[0]), `{"data":{"bar":"myValue3","baz":1000},"timestamp":"2012-01-01T02:00:00.123456Z"}`)
			assert.Equal(t, jsonenc(resp[1]), `{"data":{"bar":"myValue2","baz":20},"timestamp":"2012-01-01T03:00:00Z"}`)
		}

		// Grab a single event.
		code, resp = getJSON("/tables/foo/objects/xyz/events/2012-01-01T03:00:00Z")
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `{"data":{"bar":"myValue2","baz":20},"timestamp":"2012-01-01T03:00:00Z"}`)
	})
}

// Ensure that we receive an error when inserting a large record.
func TestServerInsertEventTooLarge(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", true, "string")

		// Send one large event (600 character string).
		code, resp := putJSON("/tables/foo/objects/xyz/events/2012-01-01T02:00:00Z", `{"data":{"bar":"012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"}}`)
		assert.Equal(t, code, 500)
		assert.Equal(t, jsonenc(resp), `{"message":"txn error: MDB_BAD_VALSIZE: Too big key/data, key is empty, or wrong DUPFIXED size"}`)
	})
}

// Ensure that we can delete all events for an object.
func TestServerEventDelete(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")

		// Send two new events.
		code, resp := putJSON("/tables/foo/objects/xyz/events/2012-01-01T02:00:00Z", `{"data":{"bar":"myValue"}}`)
		assert.Equal(t, code, 200)
		code, _ = putJSON("/tables/foo/objects/xyz/events/2012-01-01T03:00:00Z", `{"data":{"bar":"myValue2"}}`)
		assert.Equal(t, code, 200)

		// Delete one of the events.
		code, _ = deleteJSON("/tables/foo/objects/xyz/events/2012-01-01T02:00:00Z", ``)
		assert.Equal(t, code, 200)

		// Check our work.
		code, resp = getJSON("/tables/foo/objects/xyz/events")
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `[{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`)
	})
}

// Ensure that we can delete all events for an object.
func TestServerDeleteEvents(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")

		// Send two new events.
		code, resp := putJSON("/tables/foo/objects/xyz/events/2012-01-01T02:00:00Z", `{"data":{"bar":"myValue"}}`)
		assert.Equal(t, code, 200)
		code, _ = putJSON("/tables/foo/objects/xyz/events/2012-01-01T03:00:00Z", `{"data":{"bar":"myValue2"}}`)
		assert.Equal(t, code, 200)

		// Delete the events.
		code, _ = deleteJSON("/tables/foo/objects/xyz/events", ``)
		assert.Equal(t, code, 200)

		// Check our work.
		code, resp = getJSON("/tables/foo/objects/xyz/events")
		assert.Equal(t, code, 200)
		assert.Nil(t, resp)
	})
}

// Ensure that we can put multiple events on the server at once.
func TestServerStreamUpdateEvents(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")
		setupTestProperty("foo", "baz", true, "integer")

		// Send two new events in one request.
		code, resp := patchJSON("/tables/foo/events", `{"id":"xyz","timestamp":"2012-01-01T02:00:00Z","data":{"bar":"myValue", "baz":12}}{"id":"xyz","timestamp":"2012-01-01T03:00:00Z","data":{"bar":"myValue2"}}`)
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `{"count":2}`)

		// Check our work.
		code, resp = getJSON("/tables/foo/objects/xyz/events")
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `[{"data":{"bar":"myValue","baz":12},"timestamp":"2012-01-01T02:00:00Z"},{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`)
	})
}

// Ensure that we can put multiple events on the server at once, using table agnostic event stream.
func TestServerStreamUpdateEventsTableAgnostic(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo_1")
		setupTestProperty("foo_1", "bar", false, "string")
		setupTestProperty("foo_1", "baz", true, "integer")

		setupTestTable("foo_2")
		setupTestProperty("foo_2", "bar", false, "string")
		setupTestProperty("foo_2", "baz", true, "integer")

		// Send two new events in one request.
		code, resp := patchJSON("/events", `{"id":"xyz","table":"foo_1","timestamp":"2012-01-01T02:00:00Z","data":{"bar":"myValue", "baz":12}}{"id":"xyz","table":"foo_2","timestamp":"2012-01-01T02:00:00Z","data":{"bar":"myValue", "baz":12}}{"id":"xyz","table":"foo_1","timestamp":"2012-01-01T03:00:00Z","data":{"bar":"myValue2"}}{"id":"xyz","table":"foo_2","timestamp":"2012-01-01T03:00:00Z","data":{"bar":"myValue2"}}`)
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `{"count":4}`)

		// Check our work.
		code, resp = getJSON("/tables/foo_1/objects/xyz/events")
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `[{"data":{"bar":"myValue","baz":12},"timestamp":"2012-01-01T02:00:00Z"},{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`)

		code, resp = getJSON("/tables/foo_2/objects/xyz/events")
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `[{"data":{"bar":"myValue","baz":12},"timestamp":"2012-01-01T02:00:00Z"},{"data":{"bar":"myValue2"},"timestamp":"2012-01-01T03:00:00Z"}]`)
	})
}
