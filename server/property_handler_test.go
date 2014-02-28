package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that we can create a property through the server.
func TestServerPropertyCreate(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		code, resp := postJSON("/tables/foo/properties", `{"name":"bar", "transient":false, "dataType":"string"}`)
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `{"dataType":"string","id":1,"name":"bar","transient":false}`)
	})
}

// Ensure that we can retrieve all properties through the server.
func TestServerPropertyGetAll(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")
		setupTestProperty("foo", "baz", true, "integer")
		code, resp := getJSON("/tables/foo/properties")
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `[{"dataType":"integer","id":-1,"name":"baz","transient":true},{"dataType":"string","id":1,"name":"bar","transient":false}]`)
	})
}

// Ensure that we can retrieve a single property through the server.
func TestServerPropertyGet(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")
		setupTestProperty("foo", "baz", true, "integer")
		code, resp := getJSON("/tables/foo/properties/bar")
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `{"dataType":"string","id":1,"name":"bar","transient":false}`)
	})
}

// Ensure that retrieving a missing property returns an error.
func TestServerPropertyGetNotFound(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		code, resp := getJSON("/tables/foo/properties/bar")
		assert.Equal(t, code, 500)
		assert.Equal(t, jsonenc(resp), `{"message":"property not found: bar"}`)
	})
}

// Ensure that we can update a property name through the server.
func TestServerPropertyUpdate(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")
		setupTestProperty("foo", "baz", true, "integer")
		code, resp := patchJSON("/tables/foo/properties/bar", `{"name":"bat"}`)
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `{"dataType":"string","id":1,"name":"bat","transient":false}`)
	})
}

// Ensure that we can delete a property on the server.
func TestServerPropertyDelete(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")
		setupTestProperty("foo", "baz", true, "integer")
		code, resp := deleteJSON("/tables/foo/properties/bar", ``)
		assert.Equal(t, code, 200)
		code, resp = getJSON("/tables/foo/properties")
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `[{"dataType":"integer","id":-1,"name":"baz","transient":true}]`)
	})
}

// Ensure that we can delete a renamed property on the server.
func TestServerPropertyRenameAndDelete(t *testing.T) {
	runTestServer(func(s *Server) {
		setupTestTable("foo")
		setupTestProperty("foo", "bar", false, "string")
		code, resp := patchJSON("/tables/foo/properties/bar", `{"name":"bar2"}`)
		assert.Equal(t, code, 200)
		code, resp = deleteJSON("/tables/foo/properties/bar2", ``)
		assert.Equal(t, code, 200)
		code, resp = getJSON("/tables/foo/properties")
		assert.Equal(t, code, 200)
		assert.Equal(t, jsonenc(resp), `[]`)
	})
}
