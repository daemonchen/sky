package db

import (
	"testing"

	_assert "github.com/stretchr/testify/assert"
)

// Ensure that nested errors are appropriately formatted.
func TestError(t *testing.T) {
	e := &Error{"one error", &Error{"two error", nil}}
	_assert.Equal(t, e.Error(), "one error: two error")
}
