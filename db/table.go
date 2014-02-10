package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var (
	TableExistsError = &Error{"table already exists", nil}
	TableNotExistsError = &Error{"table does not exist", nil}
)

// Table is a collection of objects.
type Table struct {
	Name         string `json:"name"`
	path         string
	properties   Properties
}

// Retrieves the path on the table.
func (t *Table) Path() string {
	return t.path
}

func (t *Table) propertiesPath() string {
	return filepath.Join(t.path, "properties")
}

// Deletes a table.
func (t *Table) Delete() error {
	// Return error if the table does not exist.
	if _, err := os.Stat(t.path); os.IsNotExist(err) {
		return TableNotExistsError
	}

	// Close everything if it's open.
	if t.IsOpen() {
		t.Close()
	}

	// Delete the whole damn directory.
	os.RemoveAll(t.path)

	return nil
}

// Create initializes a new table and opens it.
func (t *Table) Create(path string) error {
	// Return error if the table already exists.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return TableExistsError
	}

	// Create root directory.
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}

	return t.Open(path)
}

// Open opens and initializes the table.
func (t *Table) Open(path string) error {
	// Return error if the table does not exist.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return TableNotExistsError
	}

	t.path = path

	// Read properties file.
	t.properties = make(Properties)
	if _, err := os.Stat(t.propertiesPath()); !os.IsNotExist(err) {
		f, err := os.Open(t.propertiesPath())
		if err != nil {
			return err
		}
		defer f.Close()

		// Decode into a properties collection.
		if err := t.properties.Decode(f); err != nil {
			return err
		}
	}

	return nil
}

// Closes the table.
func (t *Table) Close() {
	t.properties = nil
}

// Checks if the table is currently open.
func (t *Table) IsOpen() bool {
	return t.properties != nil
}

// Retrieves a reference to the current property file.
func (t *Table) Properties() Properties {
	return t.properties
}

// Adds a property to the table.
func (t *Table) CreateProperty(name string, transient bool, dataType string) (*Property, error) {
	if !t.IsOpen() {
		return nil, errors.New("Table is not open")
	}

	// Create property on property file.
	property, err := t.properties.Create(name, transient, dataType)
	if err != nil {
		return nil, err
	}

	// Save table.
	if err := t.save(); err != nil {
		return nil, err
	}

	return property, nil
}

// RenameProperty updates the name of a property.
func (t *Table) RenameProperty(oldName, newName string) (*Property, error) {
	t.properties.Rename(oldName, newName)

	if err := t.save(); err != nil {
		return nil, err
	}

	return t.properties.FindByName(newName), nil
}

// Retrieves a list of all properties on the table.
func (t *Table) GetProperties() ([]*Property, error) {
	if !t.IsOpen() {
		return nil, errors.New("Table is not open")
	}
	return t.properties.Slice(), nil
}

// Retrieves a single property from the table by id.
func (t *Table) GetProperty(id int64) (*Property, error) {
	if !t.IsOpen() {
		return nil, errors.New("Table is not open")
	}
	return t.properties.FindById(id), nil
}

// Retrieves a single property from the table by name.
func (t *Table) GetPropertyByName(name string) (*Property, error) {
	if !t.IsOpen() {
		return nil, errors.New("Table is not open")
	}
	return t.properties.FindByName(name), nil
}

// Deletes a single property on the table.
func (t *Table) DeleteProperty(property *Property) error {
	if !t.IsOpen() {
		return errors.New("Table is not open")
	}
	t.properties.Delete(property.Name)
	return t.save()
}

// Save writes the table to disk.
func (t *Table) Save() error {
	return t.save()
}

func (t *Table) save() error {
	f, err := os.Create(t.propertiesPath())
	if err != nil {
		return err
	}
	defer f.Close()
	return t.properties.Encode(f)
}

// Converts a map with string keys to use property identifier keys.
func (t *Table) NormalizeMap(m map[string]interface{}) (map[int64]interface{}, error) {
	// TODO(benbjohnson): Move normalization to table-only.
	return t.properties.NormalizeMap(m)
}

// Converts a map with property identifier keys to use string keys.
func (t *Table) DenormalizeMap(m map[int64]interface{}) (map[string]interface{}, error) {
	// TODO(benbjohnson): Move denormalization to table-only.
	return t.properties.DenormalizeMap(m)
}

// Deserializes a map into a normalized event.
func (t *Table) DeserializeEvent(m map[string]interface{}) (*Event, error) {
	event := &Event{}

	// Parse timestamp.
	if timestamp, ok := m["timestamp"].(string); ok {
		ts, err := time.Parse(time.RFC3339, timestamp)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse timestamp: %v", timestamp)
		}
		event.Timestamp = ts
	} else {
		return nil, errors.New("Timestamp required.")
	}

	// Convert maps to use property identifiers.
	if data, ok := m["data"].(map[string]interface{}); ok {
		normalizedData, err := t.NormalizeMap(data)
		if err != nil {
			return nil, err
		}
		event.Data = normalizedData
	}

	return event, nil
}

// DeserializeEvents converts denormalized key/value maps into a slice of normalized events.
func (t *Table) DeserializeEvents(items []map[string]interface{}) ([]*Event, error) {
	events := make([]*Event, 0)
	for _, item := range items {
		event, err := t.DeserializeEvent(item)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

// SerializeEvent converts a normalized event into a key/value map.
func (t *Table) SerializeEvent(event *Event) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	// Format timestamp.
	m["timestamp"] = event.Timestamp.UTC().Format(time.RFC3339Nano)

	// Convert data map to use property names.
	if event.Data != nil {
		denormalizedData, err := t.DenormalizeMap(event.Data)
		if err != nil {
			return nil, err
		}
		m["data"] = denormalizedData
	} else {
		m["data"] = map[string]interface{}{}
	}

	return m, nil
}

// SerializeEvents converts normalized events into a slice of key/value maps.
func (t *Table) SerializeEvents(events []*Event) ([]map[string]interface{}, error) {
	output := make([]map[string]interface{}, 0)
	for _, event := range events {
		item, err := t.SerializeEvent(event)
		if err != nil {
			return nil, err
		}
		output = append(output, item)
	}
	return output, nil
}
