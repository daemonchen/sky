package db_test

import (
	"testing"
	"time"

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

// Ensure that retrieving the properties of a table when it's closed returns an error.
func TestTablePropertiesNotOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		db.Close()
		p, err := table.Properties()
		assert.Equal(t, err, ErrTableNotOpen)
		assert.Nil(t, p)
	})
}

// Ensure that the table can return a map of properties by id.
func TestTablePropertiesByID(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		table.CreateProperty("prop1", String, true)
		table.CreateProperty("prop2", Factor, false)
		p, err := table.PropertiesByID()
		assert.NoError(t, err)
		assert.Equal(t, p[-1].Name, "prop1")
		assert.Equal(t, p[1].Name, "prop2")
	})
}

// Ensure that retrieving the properties of a table by id when it's closed returns an error.
func TestTablePropertiesByIDNotOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		db.Close()
		p, err := table.PropertiesByID()
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

// Ensure that retrieving an event while the database is closed returns an error.
func TestTableGetEventNotOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		db.Close()
		e, err := table.GetEvent("user1", mustParseTime("2000-01-01T00:00:01Z"))
		assert.Equal(t, err, ErrTableNotOpen)
		assert.Nil(t, e)
	})
}

// Ensure that a table can insert an event.
func TestTableInsertEvent(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		table.CreateProperty("prop1", Integer, false)
		table.CreateProperty("prop2", String, true)
		err := table.InsertEvent("user1", newEvent("2000-01-01T00:00:01Z", "prop1", 20, "prop2", "bob"))
		assert.NoError(t, err)
		err = table.InsertEvent("user2", newEvent("2000-01-01T00:00:01Z", "prop1", 100))
		assert.NoError(t, err)
		err = table.InsertEvent("user1", newEvent("2000-01-01T00:00:00Z", "prop2", "susy"))
		assert.NoError(t, err)

		// Find first user's first event.
		e, err := table.GetEvent("user1", mustParseTime("2000-01-01T00:00:01Z"))
		if assert.NoError(t, err) && assert.NotNil(t, e) {
			assert.Equal(t, e.Timestamp, mustParseTime("2000-01-01T00:00:01Z"))
			assert.Equal(t, e.Data["prop1"], int64(20))
			assert.Equal(t, e.Data["prop2"], "bob")
		}

		// Find first user's second event.
		e, err = table.GetEvent("user1", mustParseTime("2000-01-01T00:00:00Z"))
		if assert.NoError(t, err) && assert.NotNil(t, e) {
			assert.Equal(t, e.Timestamp, mustParseTime("2000-01-01T00:00:00Z"))
			assert.Nil(t, e.Data["prop1"])
			assert.Equal(t, e.Data["prop2"], "susy")
		}

		// Find second user's only event.
		e, err = table.GetEvent("user2", mustParseTime("2000-01-01T00:00:01Z"))
		if assert.NoError(t, err) && assert.NotNil(t, e) {
			assert.Equal(t, e.Timestamp, mustParseTime("2000-01-01T00:00:01Z"))
			assert.Equal(t, e.Data["prop1"], int64(100))
			assert.Nil(t, e.Data["prop2"])
		}

		// Nonexistent user shouldn't return any event.
		e, err = table.GetEvent("no-such-user", mustParseTime("2000-01-01T00:00:00Z"))
		assert.NoError(t, err)
		assert.Nil(t, e)

		// Nonexistent event shouldn't return any event.
		e, err = table.GetEvent("user1", mustParseTime("1999-01-01T00:00:00Z"))
		assert.NoError(t, err)
		assert.Nil(t, e)
	})
}

// Ensure that inserting an event into a closed table returns an error.
func TestTableInsertEventNotOpen(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		db.Close()
		err := table.InsertEvent("user1", newEvent("2000-01-01T00:00:01Z", "prop1", 20, "prop2", "bob"))
		assert.Equal(t, err, ErrTableNotOpen)
	})
}

// Ensure that a table can insert multiple events.
func TestTableInsertEvents(t *testing.T) {
	withDB(func(db *DB, path string) {
		table, _ := db.CreateTable("foo")
		table.CreateProperty("prop1", Integer, false)
		table.CreateProperty("prop2", String, true)
		err := table.InsertEvents("user1", []*Event{
			newEvent("2000-01-01T00:00:01Z", "prop1", 20, "prop2", "bob"),
			newEvent("2000-01-01T00:00:00Z", "prop2", "susy"),
		})
		assert.NoError(t, err)
		err = table.InsertEvents("user2", []*Event{
			newEvent("2000-01-01T00:00:01Z", "prop1", 100),
		})
		assert.NoError(t, err)
		err = table.InsertEvents("user3", []*Event{})

		// Find first user's events.
		events, err := table.GetEvents("user1")
		if assert.NoError(t, err) && assert.Equal(t, len(events), 2) {
			assert.Equal(t, events[0].Timestamp, mustParseTime("2000-01-01T00:00:00Z"))
			assert.Nil(t, events[0].Data["prop1"])
			assert.Equal(t, events[0].Data["prop2"], "susy")

			assert.Equal(t, events[1].Timestamp, mustParseTime("2000-01-01T00:00:01Z"))
			assert.Equal(t, events[1].Data["prop1"], int64(20))
			assert.Equal(t, events[1].Data["prop2"], "bob")
		}

		// Find second user's events.
		events, err = table.GetEvents("user2")
		if assert.NoError(t, err) && assert.Equal(t, len(events), 1) {
			assert.Equal(t, events[0].Timestamp, mustParseTime("2000-01-01T00:00:01Z"))
			assert.Equal(t, events[0].Data["prop1"], int64(100))
			assert.Nil(t, events[0].Data["prop2"])
		}

		// Third user should have no events.
		events, err = table.GetEvents("user3")
		assert.NoError(t, err)
		assert.Equal(t, len(events), 0)

		// Non-existent user should have no events.
		events, err = table.GetEvents("no-such-user")
		assert.NoError(t, err)
		assert.Equal(t, len(events), 0)
	})
}

func newEvent(timestamp string, pairs ...interface{}) *Event {
	e := &Event{Timestamp: mustParseTime(timestamp), Data: make(map[string]interface{})}
	for i := 0; i < len(pairs); i += 2 {
		e.Data[pairs[i].(string)] = pairs[i+1]
	}
	return e
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}
