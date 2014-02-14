package db

import (
	"encoding/json"
	"io"
	"sort"
)

var (
	DuplicatePropertyError = &Error{"duplicate property error", nil}
	PropertyNotFoundError  = &Error{"property not found", nil}
)

// Properties represents a list of properties on a table.
type Properties map[string]*Property

// Adds a new property to the property file and generate an identifier for it.
func (p Properties) Create(name string, transient bool, dataType string) (*Property, error) {
	// Don't allow duplicate names.
	if p[name] != nil {
		return nil, DuplicatePropertyError
	}

	// Create and validate property.
	property := &Property{Name: name, Transient: transient, DataType: dataType}
	if err := property.Validate(); err != nil {
		return nil, err
	}

	// Find the next available property id.
	if property.Transient {
		property.Id = p.nextTransientId()
	} else {
		property.Id = p.nextPermanentId()
	}

	// Add it to the collection.
	p[name] = property

	return property, nil
}

// Find retrieves a property by name.
func (p Properties) FindByName(name string) *Property {
	return p[name]
}

// Retrieves a single property by id.
func (p Properties) FindById(id int64) *Property {
	for _, property := range p {
		if property.Id == id {
			return property
		}
	}
	return nil
}

// Rename changes the name of a property.
func (p Properties) Rename(oldName, newName string) {
	if property, ok := p[oldName]; ok {
		property.Name = newName
		delete(p, oldName)
		p[newName] = property
	}
}

// Delete removes a property by name.
func (p Properties) Delete(name string) {
	delete(p, name)
}

// Slice returns a list of properties ordered by id.
func (p Properties) Slice() []*Property {
	properties := make(propertiesById, 0, len(p))
	for _, property := range p {
		properties = append(properties, property)
	}
	sort.Sort(properties)
	return []*Property(properties)
}

// Encode writes a property file to a writer.
func (p Properties) Encode(w io.Writer) error {
	return json.NewEncoder(w).Encode(p.Slice())
}

// Decode reads a property file from a reader
func (p Properties) Decode(r io.Reader) error {
	list := make([]*Property, 0)
	if err := json.NewDecoder(r).Decode(&list); err != nil {
		return err
	}

	// Create lookups for the properties.
	for _, property := range list {
		p[property.Name] = property
	}

	return nil
}

// Converts a map with string keys to use property identifier keys.
func (p Properties) NormalizeMap(m map[string]interface{}) (map[int64]interface{}, error) {
	clone := make(map[int64]interface{})
	for k, v := range m {
		// Look up the property by name and convert it to the ID.
		property := p.FindByName(string(k))
		if property != nil {
			clone[property.Id] = property.Cast(v)
		} else {
			return nil, PropertyNotFoundError
		}
	}
	return clone, nil
}

// Converts a map with property identifier keys to use string keys.
func (p Properties) DenormalizeMap(m map[int64]interface{}) (map[string]interface{}, error) {
	clone := make(map[string]interface{})
	for k, v := range m {
		// Look up the property by ID and convert it to the name.
		property := p.FindById(k)
		if property != nil {
			clone[property.Name] = v
		} else {
			return nil, PropertyNotFoundError
		}
	}
	return clone, nil
}

func (p Properties) nextTransientId() int64 {
	var id int64 = -1
	for _, property := range p {
		if property.Transient && property.Id <= id {
			id = property.Id - 1
		}
	}
	return id
}

func (p Properties) nextPermanentId() int64 {
	var id int64 = 1
	for _, property := range p {
		if !property.Transient && property.Id >= id {
			id = property.Id + 1
		}
	}
	return id
}

type propertiesById []*Property

func (s propertiesById) Len() int           { return len(s) }
func (s propertiesById) Less(i, j int) bool { return s[i].Id < s[j].Id }
func (s propertiesById) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
