package core

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that we can create a new table.
func TestTableOpen(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)
	table := &Table{Name: "test"}
	assert.NoError(t, table.Create(filepath.Join(path, "test")))
}

// Ensure that we can create properties on a table.
func TestTableCreateProperty(t *testing.T) {
	withTable(func(table *Table) {
		p, err := table.CreateProperty("name", false, "string")
		assert.NoError(t, err)
		assert.Equal(t, p.Name, "name")
		assert.Equal(t, p.Transient, false)
		assert.Equal(t, p.DataType, "string")
	})
}

func withTable(f func(*Table)) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	table := &Table{Name:"test"}
	if err := table.Open(path); err != nil {
		panic("table open error: " + err.Error())
	}

	f(table)
}

