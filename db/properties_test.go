package db

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Encode a property file.
func TestPropertiesEncode(t *testing.T) {
	p := &Properties{}
	p.Create("name", false, "string")
	p.Create("salary", false, "float")
	p.Create("purchaseAmount", true, "integer")
	p.Create("isMember", true, "boolean")

	// Encode
	buffer := new(bytes.Buffer)
	err := p.Encode(buffer)
	if err != nil {
		t.Fatalf("Unable to encode property file: %v", err)
	}
	expected := `[{"id":-2,"name":"isMember","transient":true,"dataType":"boolean"},{"id":-1,"name":"purchaseAmount","transient":true,"dataType":"integer"},{"id":1,"name":"name","transient":false,"dataType":"string"},{"id":2,"name":"salary","transient":false,"dataType":"float"}]` + "\n"
	if buffer.String() != expected {
		t.Fatalf("Invalid property file encoding:\nexp: %v\ngot: %v", expected, buffer.String())
	}
}

// Decode a property file.
func TestPropertiesDecode(t *testing.T) {
	var p = make(Properties)
	buffer := bytes.NewBufferString(`[{"id":-1,"name":"purchaseAmount","transient":true,"dataType":"integer"},{"id":1,"name":"name","transient":false,"dataType":"string"},{"id":2,"name":"salary","transient":false,"dataType":"float"}, {"id":-2,"name":"isMember","transient":true,"dataType":"boolean"}]`)
	err := p.Decode(buffer)
	assert.NoError(t, err)
	assert.Equal(t, p["isMember"], &Property{Id: -2, Name: "isMember", Transient: true, DataType: "boolean"})
	assert.Equal(t, p["purchaseAmount"], &Property{Id: -1, Name: "purchaseAmount", Transient: true, DataType: "integer"})
	assert.Equal(t, p["name"], &Property{Id: 1, Name: "name", Transient: false, DataType: "string"})
	assert.Equal(t, p["salary"], &Property{Id: 2, Name: "salary", Transient: false, DataType: "float"})
}

// Convert a map of string keys into property id keys.
func TestPropertiesNormalizeMap(t *testing.T) {
	p := &Properties{}
	p.Create("name", false, "string")
	p.Create("salary", false, "float")
	p.Create("purchaseAmount", true, "integer")

	m := map[string]interface{}{"name": "bob", "salary": 100, "purchaseAmount": 12}
	ret, err := p.NormalizeMap(m)
	assert.NoError(t, err)
	assert.Equal(t, ret[1], "bob")
	assert.Equal(t, ret[2], float64(100))
	assert.Equal(t, ret[-1], int64(12))
}

// Convert a map of string keys into property id keys.
func TestPropertiesDenormalizeMap(t *testing.T) {
	p := &Properties{}
	p.Create("name", false, "string")
	p.Create("salary", false, "float")
	p.Create("purchaseAmount", true, "integer")

	m := map[int64]interface{}{1: "bob", 2: 100, -1: 12}
	ret, err := p.DenormalizeMap(m)
	assert.NoError(t, err)
	assert.Equal(t, ret["name"], "bob")
	assert.Equal(t, ret["salary"], 100)
	assert.Equal(t, ret["purchaseAmount"], 12)
}

