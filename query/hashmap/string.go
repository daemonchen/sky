package hashmap

import (
	"hash/fnv"
)

// String generates a hash id for strings.
func String(s string) int64 {
	h := fnv.New64a()
	h.Reset()
	h.Write([]byte(s))
	value := int64(h.Sum64())
	if value < 0 {
		value *= -1
	}
	return value
}
