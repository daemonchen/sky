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

// Ensure that retrieving an event with a bad timestamp returns an error.
func TestServerEventGetWithInvalidTimestamp(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		code, resp := getJSON("/tables/foo/objects/xyz/events/bad_timestamp")
		assert.Equal(t, code, 500)
		assert.Equal(t, jsonenc(resp), `{"message":"invalid timestamp: \"bad_timestamp\""}`)
	})
}

// Ensure that inserting an event with a bad timestamp returns an error.
func TestServerEventInsertWithInvalidTimestamp(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		code, resp := putJSON("/tables/foo/objects/xyz/events/bad_timestamp", `{"data":{"bar":"myValue", "baz":12}}`)
		assert.Equal(t, code, 500)
		assert.Equal(t, jsonenc(resp), `{"message":"invalid timestamp: \"bad_timestamp\""}`)
	})
}

// Ensure that deleting an event with a bad timestamp returns an error.
func TestServerEventDeleteWithInvalidTimestamp(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		code, resp := deleteJSON("/tables/foo/objects/xyz/events/bad_timestamp", ``)
		assert.Equal(t, code, 500)
		assert.Equal(t, jsonenc(resp), `{"message":"invalid timestamp: \"bad_timestamp\""}`)
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
func TestServerTableStream(t *testing.T) {
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
func TestServerGenericStream(t *testing.T) {
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

// Ensure that streaming events to a table that doesn't exist returns an error.
func TestServerTableStreamNotFound(t *testing.T) {
	runTestServer(func(s *Server) {
		code, resp := patchJSON("/tables/foo/events", `{"id":"xyz","timestamp":"2012-01-01T02:00:00Z","data":{"bar":"myValue", "baz":12}}{"id":"xyz","timestamp":"2012-01-01T03:00:00Z","data":{"bar":"myValue2"}}`)
		assert.Equal(t, code, 404)
		assert.Equal(t, jsonenc(resp), `{"message":"table not found"}`)
	})
}

// Ensure that a malformed request body to a stream returns an error.
func TestServerTableStreamBadJSON(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		code, resp := patchJSON("/tables/foo/events", `{"id":"xyz","timestamp":"2012-01-`)
		assert.Equal(t, code, 400)
		assert.Equal(t, jsonenc(resp), `{"message":"unexpected EOF: 0"}`)
	})
}

// Ensure that streaming events without ids returns an error.
func TestServerTableStreamObjectIDRequired(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")
		setupTestProperty("foo", "baz", true, "integer")
		code, resp := patchJSON("/tables/foo/events", `{"id":"xyz","timestamp":"2012-01-01T02:00:00Z","data":{"bar":"myValue", "baz":12}}{"id":"","timestamp":"2012-01-01T03:00:00Z","data":{"bar":"myValue2"}}`)
		assert.Equal(t, code, 400)
		assert.Equal(t, jsonenc(resp), `{"message":"object id required: : 1"}`)
	})
}

// Ensure that a generic event stream with a missing table returns an error.
func TestServerGenericStreamTableNotFound(t *testing.T) {
	runTestServer(func(s *Server) {
		code, resp := patchJSON("/events", `{"id":"xyz","table":"no_such_table","timestamp":"2012-01-01T02:00:00Z","data":{"bar":"myValue", "baz":12}}{"id":"xyz","timestamp":"2012-01-01T03:00:00Z","data":{"bar":"myValue2"}}`)
		assert.Equal(t, code, 400)
		assert.Equal(t, jsonenc(resp), `{"message":"table not found: no_such_table: 0"}`)
	})
}
