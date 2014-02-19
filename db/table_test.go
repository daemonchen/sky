package db_test

import (
	"testing"

	. "github.com/skydb/sky/db"
	"github.com/stretchr/testify/assert"
)

// Ensure that a table can create new properties.
func TestTableCreateProperty(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")

		// Create permanent properties.
		p, err := table.CreateProperty("firstName", String, false)
		assert.NoError(t, err)
		assert.Equal(t, p.ID, 1)
		assert.Equal(t, p.Name, "firstName")
		assert.Equal(t, p.DataType, String)
		assert.Equal(t, p.Transient, false)

		p, err = table.CreateProperty("lastName", Factor, false)
		assert.NoError(t, err)
		assert.Equal(t, p.ID, 2)
		assert.Equal(t, p.Name, "lastName")

		// Create transient properties.
		p, err = table.CreateProperty("myNum", Integer, true)
		assert.NoError(t, err)
		assert.Equal(t, p.ID, -1)
		assert.Equal(t, p.Name, "myNum")

		p, err = table.CreateProperty("myFloat", Float, true)
		assert.NoError(t, err)
		assert.Equal(t, p.ID, -2)
		assert.Equal(t, p.Name, "myFloat")

		// Create another permanent property.
		p, err = table.CreateProperty("myBool", Float, false)
		assert.NoError(t, err)
		assert.Equal(t, p.ID, 3)
		assert.Equal(t, p.Name, "myBool")
	})
}

// Ensure that creating a property on an unopened table returns an error.
func TestTableCreatePropertyNotOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, err := db.CreateTable("foo")
		db.Close()
		p, err := table.CreateProperty("prop", Integer, false)
		assert.Equal(t, err, ErrTableNotOpen)
		assert.Nil(t, p)
	})
}

// Ensure that creating a property with an existing name returns an error.
func TestTableCreatePropertyDuplicateName(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, err := db.CreateTable("foo")
		table.CreateProperty("prop", Integer, false)
		p, err := table.CreateProperty("prop", Float, false)
		assert.Equal(t, err, ErrPropertyExists)
		assert.Nil(t, p)
	})
}

// Ensure that creating a property that fails validation will return the validation error.
func TestTableCreatePropertyInvalid(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, err := db.CreateTable("foo")
		p, err := table.CreateProperty("my•prop", Integer, false)
		assert.Equal(t, err, ErrInvalidPropertyName)
		assert.Nil(t, p)
	})
}

// Ensure that a property can be renamed.
func TestTableRenameProperty(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, err := db.CreateTable("foo")
		table.CreateProperty("prop", Integer, false)
		p, err := table.RenameProperty("prop", "prop2")
		assert.NoError(t, err)
		assert.Equal(t, p.ID, 1)
		assert.Equal(t, p.Name, "prop2")
	})
}

// Ensure that renaming a property on a closed table returns an error.
func TestTableRenamePropertyNotOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, err := db.CreateTable("foo")
		db.Close()
		p, err := table.RenameProperty("prop", "prop2")
		assert.Equal(t, err, ErrTableNotOpen)
		assert.Nil(t, p)
	})
}

// Ensure that a renaming a non-existent property returns an error.
func TestTableRenamePropertyNotFound(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, err := db.CreateTable("foo")
		p, err := table.RenameProperty("prop", "prop2")
		assert.Equal(t, err, ErrPropertyNotFound)
		assert.Nil(t, p)
	})
}

// Ensure that a renaming a property to a name that already exists returns an error.
func TestTableRenamePropertyAlreadyExists(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, err := db.CreateTable("foo")
		table.CreateProperty("prop", Integer, false)
		table.CreateProperty("prop2", Integer, false)
		p, err := table.RenameProperty("prop", "prop2")
		assert.Equal(t, err, ErrPropertyExists)
		assert.Nil(t, p)
	})
}

// Ensure that a table can delete properties.
func TestTableDeleteProperty(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		table.CreateProperty("prop1", String, false)
		table.CreateProperty("prop2", Factor, false)

		// Delete a property.
		err := table.DeleteProperty("prop2")
		assert.NoError(t, err)

		// Retrieve properties.
		p, err := table.Property("prop1")
		assert.NotNil(t, p)
		assert.NoError(t, err)
		p, err = table.Property("prop2")
		assert.Nil(t, p)
		assert.NoError(t, err)

		// Close and reopen DB.
		db.Close()
		db = &DB{}
		db.Open(path)

		// Check properties again.
		table, _ = db.OpenTable("foo")
		p, err = table.Property("prop1")
		assert.NotNil(t, p)
		p, err = table.Property("prop2")
		assert.Nil(t, p)
	})
}

// Ensure that deleting a property on a closed table returns an error.
func TestTableDeletePropertyNotOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		db.Close()
		err := table.DeleteProperty("prop2")
		assert.Equal(t, err, ErrTableNotOpen)
	})
}

// Ensure that deleting a non-existent property returns an error.
func TestTableDeletePropertyNotFound(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		err := table.DeleteProperty("prop2")
		assert.Equal(t, err, ErrPropertyNotFound)
	})
}

// Ensure that the table can return a map of properties by name.
func TestTableProperties(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		table.CreateProperty("prop1", String, true)
		table.CreateProperty("prop2", Factor, false)
		p, err := table.Properties()
		assert.NoError(t, err)
		assert.Equal(t, p["prop1"].ID, -1)
		assert.Equal(t, p["prop2"].ID, 1)
	})
}

// Ensure that retrieve the properties of a table when it's closed returns an error.
func TestTablePropertiesNotOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		db.Close()
		p, err := table.Properties()
		assert.Equal(t, err, ErrTableNotOpen)
		assert.Nil(t, p)
	})
}

// Ensure that retrieving a property from a closed table returns an error.
func TestTablePropertyNotOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		db.Close()
		p, err := table.Property("foo")
		assert.Equal(t, err, ErrTableNotOpen)
		assert.Nil(t, p)
	})
}

// Ensure that the table can retrieve a property by id.
func TestTablePropertyByID(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		table.CreateProperty("prop1", String, true)
		table.CreateProperty("prop2", Factor, false)

		p, err := table.PropertyByID(-1)
		assert.NoError(t, err)
		assert.Equal(t, p.Name, "prop1")

		p, err = table.PropertyByID(1)
		assert.NoError(t, err)
		assert.Equal(t, p.Name, "prop2")

		p, err = table.PropertyByID(2)
		assert.Nil(t, p)
		assert.Nil(t, err)
	})
}

// Ensure that retrieving a property by id from a closed table returns an error.
func TestTablePropertyByIDNotOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		db.Close()
		p, err := table.PropertyByID(-1)
		assert.Equal(t, err, ErrTableNotOpen)
		assert.Nil(t, p)
	})
}

// Ensure that a table can create properties and persist them after a reopen.
func TestTableReopen(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		table.CreateProperty("prop1", Integer, false)
		table.CreateProperty("prop2", String, true)
		table.CreateProperty("prop3", Float, false)
		table.CreateProperty("prop4", Factor, true)

		db.Close()
		assert.NoError(t, db.Open(path))

		table, _ = db.OpenTable("foo")
		p, err := table.Property("prop1")
		assert.NoError(t, err)
		assert.Equal(t, p.ID, 1)

		p, _ = table.Property("prop2")
		assert.Equal(t, p.ID, -1)
		p, _ = table.Property("prop3")
		assert.Equal(t, p.ID, 2)
		p, _ = table.Property("prop4")
		assert.Equal(t, p.ID, -2)
	})
}

/*
import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	. "github.com/skydb/sky/db"
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

	table := &Table{Name: "test"}
	if err := table.Open(path); err != nil {
		panic("table open error: " + err.Error())
	}

	f(table)
}
*/
