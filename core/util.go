package core

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
