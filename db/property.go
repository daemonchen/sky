package db

import (
	"regexp"
)

var (
	InvalidPropertyNameError     = &Error{"invalid property name", nil}
	InvalidPropertyDataTypeError = &Error{"invalid property data type", nil}
)

// Property represents part of the schema for a Table.
type Property struct {
	Id        int64  `json:"id"`
	Name      string `json:"name"`
	Transient bool   `json:"transient"`
	DataType  string `json:"dataType"`
}

// Cast converts a value into the appropriate Go type based on the property's data type.
func (p *Property) Cast(v interface{}) interface{} {
	if p.DataType == Factor || p.DataType == String {
		switch v := v.(type) {
		case string:
			return v
		default:
			return ""
		}
	} else if p.DataType == Integer {
		switch v := promote(v).(type) {
		case int64:
			return v
		case float64:
			return int64(v)
		default:
			return 0
		}
	} else if p.DataType == Float {
		switch v := promote(v).(type) {
		case float64:
			return v
		case int64:
			return float64(v)
		default:
			return 0
		}
	} else if p.DataType == Boolean {
		switch v := v.(type) {
		case bool:
			return v
		default:
			return false
		}
	}
	return v
}

// Validate checks that the property is valid. Properties can be invalid if
// non-alphanumeric characters are used in its name or if the data type is not
// a valid type.
func (p *Property) Validate() error {
	// Validate that name is non-blank and doesn't contain invalid characters.
	if p.Name == "" || !regexp.MustCompile(`^\w+$`).MatchString(p.Name) {
		return InvalidPropertyNameError
	}

	// Validate data type.
	switch p.DataType {
	case Factor, String, Integer, Float, Boolean:
	default:
		return InvalidPropertyDataTypeError
	}

	return nil
}
