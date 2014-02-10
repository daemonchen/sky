package db

import (
	"reflect"
)

// Normalizes a value. Int and Uint types are combined into int64 and Float types
// are combined into float64. All other types are left alone.
func normalize(value interface{}) interface{} {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(v.Uint())
	case reflect.Float32, reflect.Float64:
		return v.Float()
	}
	return value
}

// promote casts integer types to int64, floats to float64 and returns other types unchanged.
func promote(v interface{}) interface{} {
	switch v := v.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return v
	case float32:
		return float64(v)
	case float64:
		return float64(v)
	default:
		return v
	}
}
