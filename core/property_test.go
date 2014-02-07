package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that property names cannot be blank.
func TestPropertyValidateNameNonBlank(t *testing.T) {
	p := &Property{DataType:"string"}
	assert.Equal(t, p.Validate(), InvalidPropertyNameError)
}

// Ensure that property names cannot have invalid characters.
func TestPropertyValidateNameInvalidCharacters(t *testing.T) {
	p := &Property{Name:`foo\bar`, DataType:"string"}
	assert.Equal(t, p.Validate(), InvalidPropertyNameError)
}
