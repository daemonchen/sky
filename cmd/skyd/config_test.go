package main_test

import (
	"bytes"
	"testing"

	. "github.com/skydb/sky/cmd/skyd"
	"github.com/stretchr/testify/assert"
)

// Ensure that a configuration file can be decoded correctly.
func TestDecode(t *testing.T) {
	input := `
port=9000
data-dir="/home/data"
no-sync = true
max-dbs = 5
max-readers = 250
`

	config := NewConfig()
	err := config.Decode(bytes.NewBufferString(input))
	assert.NoError(t, err)
	assert.Equal(t, config.Port, uint(9000))
	assert.Equal(t, config.DataDir, "/home/data")
	assert.Equal(t, config.NoSync, true)
	assert.Equal(t, config.MaxDBs, uint(5))
	assert.Equal(t, config.MaxReaders, uint(250))
}

// Ensure that a badly formatted config file returns an error.
func TestDecodeBadConfig(t *testing.T) {
	input := `
port=9000
data-dir="/home
`

	config := NewConfig()
	err := config.Decode(bytes.NewBufferString(input))
	if assert.Error(t, err) {
		assert.Equal(t, err.Error(), `Near line 3, key 'data-dir': Near line 4: Strings cannot contain new lines.`)
	}
}
