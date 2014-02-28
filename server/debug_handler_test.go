package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that we can retrieve debugging information from Sky.
func TestServerDebugVars(t *testing.T) {
	runTestServer(func(s *Server) {
		code, resp := getJSON("/debug/vars")
		assert.Equal(t, code, 200)
		assert.NotNil(t, resp)
	})
}
